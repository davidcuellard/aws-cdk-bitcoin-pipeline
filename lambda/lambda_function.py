import json
import boto3
import os
from datetime import datetime, timezone, timedelta
import logging
import random
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    Generate Bitcoin market datasets.

    Modes:
    - full: generates complete history for 1w, 4h, 1d
    - incremental: generates only the last closed period for the requested interval
      event example: {"mode": "incremental", "interval": "1d"}
    """
    try:
        bucket_name = os.environ.get("DATA_LAKE_BUCKET")
        if not bucket_name:
            raise ValueError("DATA_LAKE_BUCKET environment variable not set")

        current_time = datetime.now(timezone.utc)
        s3_client = boto3.client("s3")
        s3_resource = boto3.resource("s3")

        start_of_history = datetime(2009, 1, 3, tzinfo=timezone.utc)
        end_of_history = current_time

        # Helper: approximate circulating supply for realistic market cap
        def get_btc_supply(timestamp: datetime) -> float:
            years_since_2009 = (timestamp - datetime(2009, 1, 3, tzinfo=timezone.utc)).days / 365.25
            if years_since_2009 < 4:
                return years_since_2009 * 525000
            elif years_since_2009 < 8:
                return 2100000 + (years_since_2009 - 4) * 262500
            elif years_since_2009 < 12:
                return 3150000 + (years_since_2009 - 8) * 131250
            else:
                return 4200000 + (years_since_2009 - 12) * 65625

        # Price milestone anchors across history (progress 0..1)
        price_milestones = {
            0.0: 0.01,
            0.1: 1.0,
            0.2: 10.0,
            0.3: 100.0,
            0.4: 1000.0,
            0.5: 5000.0,
            0.6: 10000.0,
            0.7: 20000.0,
            0.8: 3000.0,
            0.9: 10000.0,
            1.0: 65000.0,
        }

        def interpolate_price(progress: float) -> float:
            # Find the first milestone >= progress
            for milestone_prog, milestone_price in sorted(price_milestones.items()):
                if progress <= milestone_prog:
                    if milestone_prog == 0:
                        return milestone_price
                    prev_milestone = max([p for p in price_milestones.keys() if p < milestone_prog], default=0.0)
                    prev_price = price_milestones[prev_milestone]
                    ratio = (progress - prev_milestone) / (milestone_prog - prev_milestone)
                    return prev_price + (milestone_price - prev_price) * ratio
            return price_milestones[1.0]

        def generate_points(interval: str, start_ts: datetime, end_ts: datetime, points: int) -> list[dict]:
            processed: list[dict] = []
            total_span = (end_of_history - start_of_history).total_seconds() or 1.0

            def ts_for_index(i: int) -> datetime:
                if interval == "1w":
                    return start_ts + timedelta(weeks=i)
                if interval == "4h":
                    return start_ts + timedelta(hours=i * 4)
                return start_ts + timedelta(days=i)

            for i in range(points):
                point_ts = ts_for_index(i)
                # Clamp to requested window for safety
                if point_ts < start_ts or point_ts >= end_ts:
                    continue

                # Map absolute time to 0..1 progress across full history
                progress = max(0.0, min(1.0, (point_ts - start_of_history).total_seconds() / total_span))
                base_price = interpolate_price(progress)

                # Realistic volatility
                if progress < 0.1:
                    volatility = random.uniform(-0.50, 0.50)
                elif progress < 0.3:
                    volatility = random.uniform(-0.30, 0.30)
                elif progress < 0.7:
                    volatility = random.uniform(-0.20, 0.20)
                else:
                    volatility = random.uniform(-0.15, 0.15)

                if interval == "1w":
                    volatility *= 0.5
                elif interval == "4h":
                    volatility *= 1.5

                price = max(0.0001, base_price * (1 + volatility))

                # Volume modeled by era
                if progress < 0.1:
                    base_vol = 1_000_000
                elif progress < 0.3:
                    base_vol = 10_000_000
                elif progress < 0.7:
                    base_vol = 100_000_000
                else:
                    base_vol = 10_000_000_000
                volume = base_vol * random.uniform(0.5, 2.0)

                supply = get_btc_supply(point_ts)
                market_cap = price * supply

                if processed:
                    prev_price = processed[-1]["price"]
                    delta = price - prev_price
                    delta_pct = (delta / prev_price) * 100 if prev_price > 0 else 0
                else:
                    delta = 0
                    delta_pct = 0

                processed.append({
                    "timestamp": int(point_ts.timestamp() * 1000),
                    "timestamp_iso": point_ts.isoformat(),
                    "price": round(price, 2),
                    "market_cap": round(market_cap, 2),
                    "volume": round(volume, 2),
                    "change_24h": round(delta_pct, 2),
                    "rank": 1,
                })

            return processed

        mode = (event or {}).get("mode", "full") if isinstance(event, dict) else "full"
        requested_interval = (event or {}).get("interval") if isinstance(event, dict) else None
        wipe_prefix = (event or {}).get("wipe_prefix") if isinstance(event, dict) else None

        # Optional cleanup step
        if wipe_prefix:
            logger.info(f"Wiping s3://{bucket_name}/{wipe_prefix} ...")
            bucket = s3_resource.Bucket(bucket_name)
            # Safety: restrict deletes to the provided prefix only
            to_delete = bucket.objects.filter(Prefix=wipe_prefix)
            # Batch delete in chunks for large listings
            batch = []
            for obj in to_delete:
                batch.append({"Key": obj.key})
                if len(batch) == 1000:
                    s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": batch})
                    batch = []
            if batch:
                s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": batch})
            logger.info(f"Wipe complete for prefix: {wipe_prefix}")

        results = []

        if mode == "incremental":
            if requested_interval not in {"1d", "4h", "1w"}:
                raise ValueError("incremental mode requires interval in {'1d','4h','1w'}")

            if requested_interval == "1d":
                # Previous full UTC day
                day_end = datetime(current_time.year, current_time.month, current_time.day, tzinfo=timezone.utc)
                day_start = day_end - timedelta(days=1)
                points = 1
                start_ts, end_ts = day_start, day_end
            elif requested_interval == "4h":
                # Last closed 4h window
                hour = (current_time.hour // 4) * 4
                window_end = datetime(current_time.year, current_time.month, current_time.day, hour, tzinfo=timezone.utc)
                window_start = window_end - timedelta(hours=4)
                points = 1
                start_ts, end_ts = window_start, window_end
            else:
                # Last full week ending Sunday 00:00 UTC
                # Compute start of this week (Monday 00:00), then go back 1 week
                dow = current_time.weekday()  # Monday=0
                start_of_this_week = datetime(current_time.year, current_time.month, current_time.day, tzinfo=timezone.utc) - timedelta(days=dow)
                week_end = datetime(start_of_this_week.year, start_of_this_week.month, start_of_this_week.day, tzinfo=timezone.utc)
                week_start = week_end - timedelta(days=7)
                points = 1
                start_ts, end_ts = week_start, week_end

            # Build deterministic S3 key based on the logical window end to avoid duplicates
            if requested_interval == "4h":
                key_stamp = end_ts.strftime("%Y%m%d_%H%M")
            else:
                key_stamp = end_ts.strftime("%Y%m%d")

            year = end_ts.strftime("%Y")
            month = end_ts.strftime("%m")
            day = end_ts.strftime("%d")
            s3_key = (
                f"silver/interval={requested_interval}/ingestion_date={year}/{month}/{day}/"
                f"bitcoin_market_{requested_interval}_{key_stamp}.json"
            )

            # Idempotency: skip if this window already exists
            try:
                s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                logger.info(f"Incremental window already written, skipping: s3://{bucket_name}/{s3_key}")
                return {
                    "statusCode": 200,
                    "body": json.dumps({
                        "message": "Incremental window already exists, skipping",
                        "mode": mode,
                        "interval": requested_interval,
                        "s3_path": f"s3://{bucket_name}/{s3_key}",
                    }),
                }
            except ClientError as e:
                if e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") not in (403, 404):
                    # For 403/404, treat as not found; otherwise re-raise
                    raise

            processed_data = generate_points(requested_interval, start_ts, end_ts, points)

            latest = processed_data[-1] if processed_data else None
            current_price = latest["price"] if latest else 0
            current_market_cap = latest["market_cap"] if latest else 0
            total_volume = sum(d["volume"] for d in processed_data) if processed_data else 0
            avg_price = (sum(d["price"] for d in processed_data) / len(processed_data)) if processed_data else 0
            highest_price = max((d["price"] for d in processed_data), default=0)
            lowest_price = min((d["price"] for d in processed_data), default=0)
            price_change = 0
            price_change_pct = 0

            payload = {
                "ingestion_timestamp": current_time.isoformat(),
                "symbol": "BTC",
                "currency": "USD",
                "interval": requested_interval,
                "record_count": len(processed_data),
                "data_source": "synthetic",
                "current_price": round(current_price, 2),
                "current_market_cap": round(current_market_cap, 2),
                "price_change": round(price_change, 2),
                "price_change_percent": round(price_change_pct, 2),
                "total_volume": round(total_volume, 2),
                "average_price": round(avg_price, 2),
                "highest_price": round(highest_price, 2),
                "lowest_price": round(lowest_price, 2),
                "market_data": processed_data,
            }

            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=json.dumps(payload, separators=(",", ":")),
                ContentType="application/json",
            )

            results.append({
                "interval": requested_interval,
                "records_written": len(processed_data),
                "s3_path": f"s3://{bucket_name}/{s3_key}",
                "description": f"Incremental {requested_interval}",
            })

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Incremental write complete",
                    "mode": mode,
                    "datasets": results,
                })
            }

        # Full history generation
        total_days = (end_of_history - start_of_history).days
        total_hours = total_days * 24
        datasets = {
            "1w": {"interval": "1w", "points": total_days // 7, "description": f"Weekly data ({total_days // 7} points)"},
            "4h": {"interval": "4h", "points": total_hours // 4, "description": f"4-hourly data ({total_hours // 4} points)"},
            "1d": {"interval": "1d", "points": total_days, "description": f"Daily data ({total_days} points)"},
        }

        for _, cfg in datasets.items():
            interval = cfg["interval"]
            # Define window for full history
            if interval == "1w":
                start_ts = start_of_history
                end_ts = end_of_history
            elif interval == "4h":
                start_ts = start_of_history
                end_ts = end_of_history
            else:
                start_ts = start_of_history
                end_ts = end_of_history

            processed = generate_points(interval, start_ts, end_ts, cfg["points"])

            if processed:
                latest = processed[-1]
                current_price = latest["price"]
                current_market_cap = latest["market_cap"]
                total_volume = sum(d["volume"] for d in processed)
                avg_price = sum(d["price"] for d in processed) / len(processed)
                highest_price = max(d["price"] for d in processed)
                lowest_price = min(d["price"] for d in processed)
                recent_start = max(0, len(processed) - len(processed) // 10)
                if recent_start < len(processed) - 1:
                    first_recent = processed[recent_start]
                    price_change = current_price - first_recent["price"]
                    price_change_pct = (price_change / first_recent["price"]) * 100 if first_recent["price"] > 0 else 0
                else:
                    price_change = 0
                    price_change_pct = 0
            else:
                current_price = 0
                current_market_cap = 0
                total_volume = 0
                avg_price = 0
                highest_price = 0
                lowest_price = 0
                price_change = 0
                price_change_pct = 0

            year = current_time.strftime("%Y")
            month = current_time.strftime("%m")
            day = current_time.strftime("%d")
            timestamp = current_time.strftime("%Y%m%d_%H%M%S")
            s3_key = f"silver/interval={interval}/ingestion_date={year}/{month}/{day}/bitcoin_market_{interval}_{timestamp}.json"

            payload = {
                "ingestion_timestamp": current_time.isoformat(),
                "symbol": "BTC",
                "currency": "USD",
                "interval": interval,
                "record_count": len(processed),
                "data_source": "synthetic",
                "current_price": round(current_price, 2),
                "current_market_cap": round(current_market_cap, 2),
                "price_change": round(price_change, 2),
                "price_change_percent": round(price_change_pct, 2),
                "total_volume": round(total_volume, 2),
                "average_price": round(avg_price, 2),
                "highest_price": round(highest_price, 2),
                "lowest_price": round(lowest_price, 2),
                "market_data": processed,
            }

            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=json.dumps(payload, separators=(",", ":")),
                ContentType="application/json",
            )

            results.append({
                "interval": interval,
                "records_written": len(processed),
                "s3_path": f"s3://{bucket_name}/{s3_key}",
                "description": cfg["description"],
            })

        total_records = sum(r["records_written"] for r in results)
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Generated {total_records} Bitcoin data points across {len(results)} datasets",
                "mode": "full",
                "total_records": total_records,
                "datasets": results,
                "time_range": f"{start_of_history.strftime('%Y-%m-%d')} to {end_of_history.strftime('%Y-%m-%d')}",
            })
        }

    except Exception as e:
        logger.error(f"Error extracting Bitcoin market data: {str(e)}")
        raise e


