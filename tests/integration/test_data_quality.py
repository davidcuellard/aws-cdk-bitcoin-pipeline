"""
Data quality tests for the Bitcoin data pipeline
Tests data accuracy, completeness, and consistency
"""

import boto3
import json
import pytest
from datetime import datetime, timezone
from botocore.exceptions import ClientError


class TestDataQuality:
    """Data quality tests for the Bitcoin data pipeline"""

    @pytest.fixture(scope="class")
    def aws_session(self):
        """Create AWS session for testing"""
        return boto3.Session(region_name='us-east-1')

    @pytest.fixture(scope="class")
    def s3_client(self, aws_session):
        """Create S3 client"""
        return aws_session.client('s3')

    @pytest.fixture(scope="class")
    def athena_client(self, aws_session):
        """Create Athena client"""
        return aws_session.client('athena')

    def test_data_completeness(self, s3_client):
        """Test that all required data is present"""
        bucket_name = 'data-pipeline-datalake-055533307082-us-east-1'
        
        try:
            # List all objects in silver folder
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='silver/'
            )
            
            assert 'Contents' in response
            objects = response['Contents']
            
            # Check for all three intervals
            intervals_found = set()
            for obj in objects:
                if 'interval=1w/' in obj['Key']:
                    intervals_found.add('1w')
                elif 'interval=4h/' in obj['Key']:
                    intervals_found.add('4h')
                elif 'interval=1d/' in obj['Key']:
                    intervals_found.add('1d')
            
            assert '1w' in intervals_found, "Weekly data not found"
            assert '4h' in intervals_found, "4-hourly data not found"
            assert '1d' in intervals_found, "Daily data not found"
            
            print(f"✅ Data completeness verified: {len(intervals_found)} intervals found")
            
        except ClientError as e:
            pytest.fail(f"Data completeness test failed: {e}")

    def test_data_structure(self, s3_client):
        """Test that data has correct structure"""
        bucket_name = 'data-pipeline-datalake-055533307082-us-east-1'
        
        try:
            # Get a sample file from each interval
            intervals = ['1w', '4h', '1d']
            
            for interval in intervals:
                # List objects for this interval
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=f'silver/interval={interval}/'
                )
                
                if 'Contents' in response and len(response['Contents']) > 0:
                    # Get the first file
                    sample_key = response['Contents'][0]['Key']
                    
                    # Download and parse the file
                    file_response = s3_client.get_object(Bucket=bucket_name, Key=sample_key)
                    content = file_response['Body'].read().decode('utf-8')
                    data = json.loads(content)
                    
                    # Verify top-level structure
                    required_fields = [
                        'ingestion_timestamp', 'symbol', 'currency', 'interval',
                        'record_count', 'current_price', 'current_market_cap',
                        'price_change', 'price_change_percent', 'total_volume',
                        'average_price', 'highest_price', 'lowest_price', 'market_data'
                    ]
                    
                    for field in required_fields:
                        assert field in data, f"Field {field} missing in {interval} data"
                    
                    # Verify data types
                    assert isinstance(data['symbol'], str)
                    assert isinstance(data['currency'], str)
                    assert isinstance(data['interval'], str)
                    assert isinstance(data['record_count'], int)
                    assert isinstance(data['current_price'], (int, float))
                    assert isinstance(data['market_data'], list)
                    
                    # Verify market_data structure
                    if len(data['market_data']) > 0:
                        first_point = data['market_data'][0]
                        required_market_fields = ['timestamp', 'price', 'volume', 'market_cap']
                        
                        for field in required_market_fields:
                            assert field in first_point, f"Market data field {field} missing in {interval} data"
                    
                    print(f"✅ Data structure verified for {interval} interval")
                else:
                    pytest.fail(f"No data found for {interval} interval")
                    
        except ClientError as e:
            pytest.fail(f"Data structure test failed: {e}")

    def test_data_accuracy(self, s3_client):
        """Test that data values are accurate and realistic"""
        bucket_name = 'data-pipeline-datalake-055533307082-us-east-1'
        
        try:
            # Get a sample file
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='silver/',
                MaxKeys=1
            )
            
            if 'Contents' in response and len(response['Contents']) > 0:
                sample_key = response['Contents'][0]['Key']
                
                # Download and parse the file
                file_response = s3_client.get_object(Bucket=bucket_name, Key=sample_key)
                content = file_response['Body'].read().decode('utf-8')
                data = json.loads(content)
                
                # Verify Bitcoin symbol
                assert data['symbol'] == 'BTC'
                assert data['currency'] == 'USD'
                
                # Verify price is positive
                assert data['current_price'] > 0
                assert data['current_market_cap'] > 0
                assert data['total_volume'] > 0
                
                # Verify price change percentage is reasonable
                assert abs(data['price_change_percent']) < 10000  # Less than 10,000%
                
                # Verify market data points
                if len(data['market_data']) > 0:
                    for point in data['market_data'][:5]:  # Check first 5 points
                        assert point['price'] > 0
                        assert point['volume'] > 0
                        # Market cap can be 0 for very early Bitcoin data (2009-2010)
                        assert point['market_cap'] >= 0
                        
                        # Verify timestamp format
                        timestamp = point['timestamp']
                        # Timestamp can be either string (ISO format) or number (Unix timestamp)
                        assert isinstance(timestamp, (str, int, float))
                        if isinstance(timestamp, str):
                            # Should be ISO format
                            assert 'T' in timestamp
                            assert 'Z' in timestamp or '+' in timestamp
                        else:
                            # Should be Unix timestamp (positive number)
                            assert timestamp > 0
                
                print("✅ Data accuracy verified")
            else:
                pytest.fail("No data files found for accuracy testing")
                
        except ClientError as e:
            pytest.fail(f"Data accuracy test failed: {e}")

    def test_data_consistency(self, s3_client):
        """Test that data is consistent across intervals"""
        bucket_name = 'data-pipeline-datalake-055533307082-us-east-1'
        
        try:
            # Get sample files from each interval
            intervals = ['1w', '4h', '1d']
            interval_data = {}
            
            for interval in intervals:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=f'silver/interval={interval}/'
                )
                
                if 'Contents' in response and len(response['Contents']) > 0:
                    sample_key = response['Contents'][0]['Key']
                    
                    # Download and parse the file
                    file_response = s3_client.get_object(Bucket=bucket_name, Key=sample_key)
                    content = file_response['Body'].read().decode('utf-8')
                    data = json.loads(content)
                    
                    interval_data[interval] = data
            
            # Verify consistency
            if len(interval_data) > 1:
                # All should have same symbol and currency
                symbols = [data['symbol'] for data in interval_data.values()]
                currencies = [data['currency'] for data in interval_data.values()]
                
                assert len(set(symbols)) == 1, "Inconsistent symbols across intervals"
                assert len(set(currencies)) == 1, "Inconsistent currencies across intervals"
                
                # All should have positive values
                for interval, data in interval_data.items():
                    assert data['current_price'] > 0, f"Invalid price in {interval} data"
                    assert data['current_market_cap'] > 0, f"Invalid market cap in {interval} data"
                    assert data['total_volume'] > 0, f"Invalid volume in {interval} data"
                
                print(f"✅ Data consistency verified across {len(interval_data)} intervals")
            else:
                pytest.fail("Not enough interval data for consistency testing")
                
        except ClientError as e:
            pytest.fail(f"Data consistency test failed: {e}")

    def test_data_volume(self, s3_client):
        """Test that data volume is appropriate"""
        bucket_name = 'data-pipeline-datalake-055533307082-us-east-1'
        
        try:
            # List all objects
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='silver/'
            )
            
            if 'Contents' in response:
                objects = response['Contents']
                
                # Check file sizes
                total_size = 0
                for obj in objects:
                    total_size += obj['Size']
                
                # Total size should be reasonable (not too small, not too large)
                assert total_size > 1024, "Total data size too small"  # At least 1KB
                assert total_size < 100 * 1024 * 1024, "Total data size too large"  # Less than 100MB
                
                # Check individual file sizes
                for obj in objects:
                    if obj['Key'].endswith('.json'):
                        assert obj['Size'] > 0, f"Empty file: {obj['Key']}"
                        assert obj['Size'] < 50 * 1024 * 1024, f"File too large: {obj['Key']}"  # Less than 50MB
                
                print(f"✅ Data volume verified: {len(objects)} files, {total_size} bytes total")
            else:
                pytest.fail("No data files found for volume testing")
                
        except ClientError as e:
            pytest.fail(f"Data volume test failed: {e}")

    def test_data_freshness(self, s3_client):
        """Test that data is fresh (recently generated)"""
        bucket_name = 'data-pipeline-datalake-055533307082-us-east-1'
        
        try:
            # List all objects
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='silver/'
            )
            
            if 'Contents' in response:
                objects = response['Contents']
                current_time = datetime.now(timezone.utc)
                
                # Check that files are recent (within last 24 hours)
                for obj in objects:
                    if obj['Key'].endswith('.json'):
                        file_time = obj['LastModified']
                        time_diff = (current_time - file_time).total_seconds()
                        
                        assert time_diff < 86400, f"File too old: {obj['Key']}"  # Less than 24 hours
                
                print(f"✅ Data freshness verified: {len(objects)} recent files")
            else:
                pytest.fail("No data files found for freshness testing")
                
        except ClientError as e:
            pytest.fail(f"Data freshness test failed: {e}")

    def test_data_partitioning(self, s3_client):
        """Test that data is properly partitioned"""
        bucket_name = 'data-pipeline-datalake-055533307082-us-east-1'
        
        try:
            # List all objects
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='silver/'
            )
            
            if 'Contents' in response:
                objects = response['Contents']
                
                # Check partitioning structure
                partition_patterns = {
                    'interval': set(),
                    'ingestion_date': set()
                }
                
                for obj in objects:
                    key = obj['Key']
                    
                    # Check for interval partitioning
                    if 'interval=' in key:
                        interval = key.split('interval=')[1].split('/')[0]
                        partition_patterns['interval'].add(interval)
                    
                    # Check for date partitioning
                    if 'ingestion_date=' in key:
                        date_part = key.split('ingestion_date=')[1].split('/')[0]
                        partition_patterns['ingestion_date'].add(date_part)
                
                # Verify partitioning
                assert len(partition_patterns['interval']) > 0, "No interval partitioning found"
                assert len(partition_patterns['ingestion_date']) > 0, "No date partitioning found"
                
                print(f"✅ Data partitioning verified: {len(partition_patterns['interval'])} intervals, {len(partition_patterns['ingestion_date'])} dates")
            else:
                pytest.fail("No data files found for partitioning testing")
                
        except ClientError as e:
            pytest.fail(f"Data partitioning test failed: {e}")

    def test_data_encoding(self, s3_client):
        """Test that data is properly encoded"""
        bucket_name = 'data-pipeline-datalake-055533307082-us-east-1'
        
        try:
            # Get a sample file
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='silver/',
                MaxKeys=1
            )
            
            if 'Contents' in response and len(response['Contents']) > 0:
                sample_key = response['Contents'][0]['Key']
                
                # Download the file
                file_response = s3_client.get_object(Bucket=bucket_name, Key=sample_key)
                content = file_response['Body'].read()
                
                # Try to decode as UTF-8
                try:
                    decoded_content = content.decode('utf-8')
                    
                    # Try to parse as JSON
                    data = json.loads(decoded_content)
                    
                    # Verify it's valid JSON
                    assert isinstance(data, dict)
                    
                    print("✅ Data encoding verified: Valid UTF-8 JSON")
                except UnicodeDecodeError:
                    pytest.fail("Data encoding test failed: Invalid UTF-8 encoding")
                except json.JSONDecodeError:
                    pytest.fail("Data encoding test failed: Invalid JSON format")
            else:
                pytest.fail("No data files found for encoding testing")
                
        except ClientError as e:
            pytest.fail(f"Data encoding test failed: {e}")

    def test_data_metadata(self, s3_client):
        """Test that data has proper metadata"""
        bucket_name = 'data-pipeline-datalake-055533307082-us-east-1'
        
        try:
            # Get a sample file
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='silver/',
                MaxKeys=1
            )
            
            if 'Contents' in response and len(response['Contents']) > 0:
                sample_key = response['Contents'][0]['Key']
                
                # Get object metadata
                head_response = s3_client.head_object(Bucket=bucket_name, Key=sample_key)
                
                # Check metadata
                assert 'ContentType' in head_response
                assert 'ContentLength' in head_response
                assert 'LastModified' in head_response
                
                # Verify content type
                content_type = head_response.get('ContentType', '')
                assert 'application/json' in content_type or 'text/plain' in content_type
                
                # Verify content length
                content_length = head_response['ContentLength']
                assert content_length > 0
                
                print("✅ Data metadata verified")
            else:
                pytest.fail("No data files found for metadata testing")
                
        except ClientError as e:
            pytest.fail(f"Data metadata test failed: {e}")


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
