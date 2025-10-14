-- 🧪 Complete Bitcoin Historical Data Pipeline - SQL Queries
-- This file contains comprehensive SQL queries to test and analyze your Bitcoin historical data
-- ============================================================================
-- BASIC DATA EXPLORATION QUERIES
-- ============================================================================
-- 1. Count total records
SELECT
    COUNT(*) as total_records
FROM
    data_pipeline_analytics.bitcoin_data;

-- 2. See all intervals available
SELECT DISTINCT
    interval
FROM
    data_pipeline_analytics.bitcoin_data;

-- 3. Check data distribution by interval
SELECT
    interval,
    COUNT(*) as record_count,
    MIN(ingestion_timestamp) as earliest,
    MAX(ingestion_timestamp) as latest
FROM
    data_pipeline_analytics.bitcoin_data
GROUP BY
    interval;

-- 4. View sample data
SELECT
    *
FROM
    data_pipeline_analytics.bitcoin_data
LIMIT
    10;

-- ============================================================================
-- BITCOIN MARKET ANALYSIS QUERIES
-- ============================================================================
-- 5. Get latest Bitcoin prices by interval
SELECT
    interval,
    current_price,
    current_market_cap,
    price_change_percent,
    record_count,
    ingestion_timestamp
FROM
    data_pipeline_analytics.bitcoin_data
ORDER BY
    interval;

-- 6. Find highest and lowest prices by interval
SELECT
    interval,
    highest_price,
    lowest_price,
    average_price,
    (highest_price - lowest_price) as price_range
FROM
    data_pipeline_analytics.bitcoin_data;

-- 7. Volume analysis by interval
SELECT
    interval,
    total_volume,
    current_price,
    record_count,
    (total_volume / record_count) as avg_volume_per_record
FROM
    data_pipeline_analytics.bitcoin_data;

-- 8. Market cap analysis
SELECT
    interval,
    current_market_cap,
    current_price,
    record_count,
    (current_market_cap / current_price) as estimated_supply
FROM
    data_pipeline_analytics.bitcoin_data;

-- ============================================================================
-- PARTITION-BASED QUERIES
-- ============================================================================
-- 9. Query specific interval (daily data)
SELECT
    interval,
    record_count,
    current_price,
    price_change_percent,
    total_volume,
    ingestion_timestamp
FROM
    data_pipeline_analytics.bitcoin_data
WHERE
    interval = '1d';

-- 10. Query weekly data
SELECT
    interval,
    record_count,
    current_price,
    average_price,
    price_change_percent,
    ingestion_timestamp
FROM
    data_pipeline_analytics.bitcoin_data
WHERE
    interval = '1w';

-- 11. Query 4-hourly data
SELECT
    interval,
    record_count,
    current_price,
    highest_price,
    lowest_price,
    ingestion_timestamp
FROM
    data_pipeline_analytics.bitcoin_data
WHERE
    interval = '4h';

-- ============================================================================
-- PERFORMANCE AND AGGREGATION QUERIES
-- ============================================================================
-- 12. Summary statistics by interval
SELECT
    interval,
    COUNT(*) as datasets,
    AVG(current_price) as avg_price,
    MAX(highest_price) as max_price,
    MIN(lowest_price) as min_price,
    SUM(total_volume) as total_volume,
    AVG(price_change_percent) as avg_price_change_percent
FROM
    data_pipeline_analytics.bitcoin_data
GROUP BY
    interval;

-- 13. Price change analysis
SELECT
    interval,
    current_price,
    price_change,
    price_change_percent,
    CASE
        WHEN price_change_percent > 0 THEN 'Positive'
        WHEN price_change_percent < 0 THEN 'Negative'
        ELSE 'Neutral'
    END as trend,
    ingestion_timestamp
FROM
    data_pipeline_analytics.bitcoin_data
ORDER BY
    interval,
    ingestion_timestamp DESC;

-- 14. Volatility analysis
SELECT
    interval,
    current_price,
    average_price,
    highest_price,
    lowest_price,
    (
        (highest_price - lowest_price) / average_price * 100
    ) as volatility_percent,
    price_change_percent
FROM
    data_pipeline_analytics.bitcoin_data;

-- ============================================================================
-- DATA QUALITY CHECKS
-- ============================================================================
-- 15. Check data completeness
SELECT
    interval,
    COUNT(*) as record_count,
    MIN(ingestion_timestamp) as earliest,
    MAX(ingestion_timestamp) as latest,
    COUNT(DISTINCT DATE (ingestion_timestamp)) as unique_dates
FROM
    data_pipeline_analytics.bitcoin_data
GROUP BY
    interval;

-- 16. Verify data source and consistency
SELECT DISTINCT
    data_source,
    symbol,
    currency,
    COUNT(*) as count
FROM
    data_pipeline_analytics.bitcoin_data
GROUP BY
    data_source,
    symbol,
    currency;

-- 17. Check for null or invalid values
SELECT
    interval,
    COUNT(*) as total_records,
    COUNT(
        CASE
            WHEN current_price IS NULL
            OR current_price <= 0 THEN 1
        END
    ) as invalid_prices,
    COUNT(
        CASE
            WHEN record_count IS NULL
            OR record_count <= 0 THEN 1
        END
    ) as invalid_counts,
    COUNT(
        CASE
            WHEN total_volume IS NULL
            OR total_volume < 0 THEN 1
        END
    ) as invalid_volumes
FROM
    data_pipeline_analytics.bitcoin_data
GROUP BY
    interval;

-- ============================================================================
-- ADVANCED ANALYTICS QUERIES
-- ============================================================================
-- 18. Bitcoin price evolution over time (by interval)
SELECT
    interval,
    record_count,
    current_price,
    average_price,
    (current_price / average_price - 1) * 100 as price_vs_average_percent,
    price_change_percent,
    ingestion_timestamp
FROM
    data_pipeline_analytics.bitcoin_data
ORDER BY
    interval,
    ingestion_timestamp DESC;

-- 19. Market cap vs price correlation
SELECT
    interval,
    current_price,
    current_market_cap,
    (current_market_cap / current_price) as estimated_supply,
    total_volume,
    (total_volume / current_market_cap * 100) as volume_to_market_cap_ratio
FROM
    data_pipeline_analytics.bitcoin_data;

-- 20. Price performance comparison across intervals
SELECT
    interval,
    current_price,
    average_price,
    highest_price,
    lowest_price,
    (current_price - average_price) as price_deviation,
    (
        (current_price - average_price) / average_price * 100
    ) as price_deviation_percent,
    price_change_percent
FROM
    data_pipeline_analytics.bitcoin_data
ORDER BY
    price_deviation_percent DESC;

-- ============================================================================
-- BUSINESS INTELLIGENCE QUERIES
-- ============================================================================
-- 21. Bitcoin market summary dashboard
SELECT
    'Bitcoin Historical Data Summary' as title,
    COUNT(*) as total_datasets,
    COUNT(DISTINCT interval) as time_intervals,
    MIN(ingestion_timestamp) as data_start_time,
    MAX(ingestion_timestamp) as data_end_time,
    AVG(current_price) as avg_bitcoin_price,
    MAX(highest_price) as all_time_high,
    MIN(lowest_price) as all_time_low,
    SUM(total_volume) as total_volume_traded
FROM
    data_pipeline_analytics.bitcoin_data;

-- 22. Risk analysis by interval
SELECT
    interval,
    current_price,
    average_price,
    highest_price,
    lowest_price,
    (
        (highest_price - current_price) / current_price * 100
    ) as downside_risk_percent,
    (
        (current_price - lowest_price) / current_price * 100
    ) as upside_potential_percent,
    price_change_percent
FROM
    data_pipeline_analytics.bitcoin_data;

-- 23. Volume analysis by interval
SELECT
    interval,
    total_volume,
    current_price,
    (total_volume / current_price) as volume_in_bitcoin_equivalent,
    record_count,
    (total_volume / record_count) as avg_volume_per_data_point
FROM
    data_pipeline_analytics.bitcoin_data
ORDER BY
    total_volume DESC;

-- ============================================================================
-- TESTING AND VALIDATION QUERIES
-- ============================================================================
-- 24. Data integrity check
SELECT
    interval,
    COUNT(*) as total_records,
    COUNT(
        CASE
            WHEN current_price > 0 THEN 1
        END
    ) as valid_prices,
    COUNT(
        CASE
            WHEN current_market_cap > 0 THEN 1
        END
    ) as valid_market_caps,
    COUNT(
        CASE
            WHEN total_volume >= 0 THEN 1
        END
    ) as valid_volumes,
    COUNT(
        CASE
            WHEN record_count > 0 THEN 1
        END
    ) as valid_counts
FROM
    data_pipeline_analytics.bitcoin_data
GROUP BY
    interval;

-- 25. Schema validation
SELECT
    'Schema Check' as test_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT interval) as unique_intervals,
    COUNT(DISTINCT symbol) as unique_symbols,
    COUNT(DISTINCT currency) as unique_currencies,
    COUNT(DISTINCT data_source) as unique_data_sources
FROM
    data_pipeline_analytics.bitcoin_data;

-- 26. Performance test query
SELECT
    interval,
    COUNT(*) as record_count,
    AVG(current_price) as avg_price,
    MAX(current_price) as max_price,
    MIN(current_price) as min_price,
    STDDEV (current_price) as price_stddev,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY
            current_price
    ) as median_price
FROM
    data_pipeline_analytics.bitcoin_data
GROUP BY
    interval
ORDER BY
    record_count DESC;

-- ============================================================================
-- DEMONSTRATION QUERIES FOR TECHNICAL TEST
-- ============================================================================
-- 27. Quick demonstration query (for technical test)
SELECT
    'Bitcoin Historical Data Pipeline' as pipeline_name,
    COUNT(*) as total_datasets,
    STRING_AGG (DISTINCT interval, ', ') as available_intervals,
    MIN(ingestion_timestamp) as data_start,
    MAX(ingestion_timestamp) as data_end,
    ROUND(AVG(current_price), 2) as avg_bitcoin_price,
    ROUND(MAX(highest_price), 2) as all_time_high,
    ROUND(MIN(lowest_price), 2) as all_time_low
FROM
    data_pipeline_analytics.bitcoin_data;

-- 28. Data pipeline health check
SELECT
    CASE
        WHEN COUNT(*) >= 3 THEN 'HEALTHY'
        ELSE 'NEEDS_ATTENTION'
    END as pipeline_status,
    COUNT(*) as dataset_count,
    COUNT(DISTINCT interval) as interval_count,
    MIN(record_count) as min_records,
    MAX(record_count) as max_records,
    AVG(record_count) as avg_records
FROM
    data_pipeline_analytics.bitcoin_data;

-- 29. Technical test validation
SELECT
    'Technical Test Validation' as test_type,
    CASE
        WHEN COUNT(*) >= 3
        AND COUNT(DISTINCT interval) >= 3 THEN 'PASSED'
        ELSE 'FAILED'
    END as test_result,
    COUNT(*) as total_datasets,
    COUNT(DISTINCT interval) as unique_intervals,
    STRING_AGG (DISTINCT interval, ', ') as intervals_found,
    SUM(record_count) as total_data_points
FROM
    data_pipeline_analytics.bitcoin_data;

-- ============================================================================
-- USAGE INSTRUCTIONS
-- ============================================================================
/*
HOW TO USE THESE QUERIES:

1. **Basic Testing**: Start with queries 1-4 to verify data exists
2. **Data Exploration**: Use queries 5-8 to understand your data
3. **Partition Testing**: Use queries 9-11 to test different intervals
4. **Analytics**: Use queries 12-20 for deeper analysis
5. **Quality Assurance**: Use queries 21-26 for data validation
6. **Demonstration**: Use queries 27-29 for technical test presentation

QUERY EXECUTION:
1. Go to Athena Console: https://us-east-1.console.aws.amazon.com/athena/
2. Select workgroup: data-pipeline-analytics
3. Copy and paste any query from this file
4. Click "Run" to execute

EXPECTED RESULTS:
- Query 1: Should return 3 records (one for each interval)
- Query 2: Should show '1d', '4h', '1w' intervals
- Query 3: Should show record counts: ~6126 (daily), ~36756 (4h), ~875 (weekly)
- Query 27: Should show comprehensive pipeline summary
- Query 29: Should show "PASSED" for technical test validation

TROUBLESHOOTING:
- If queries fail, check that Glue Crawler has completed successfully
- If no data, run: aws glue start-crawler --name data-pipeline-crawler
- If permission errors, check Lake Formation permissions
- If table not found, verify table name is 'bitcoin_data'

PERFORMANCE TIPS:
- Use WHERE clauses to filter by interval for faster queries
- Use LIMIT clauses for large result sets
- Check query execution time in Athena console
- Use appropriate data types for aggregations
 */