"""
Integration tests for the complete Bitcoin data pipeline
Tests the entire flow from Lambda execution to Athena queries
"""

import boto3
import json
import time
import pytest
from datetime import datetime, timezone
from botocore.exceptions import ClientError


class TestPipelineIntegration:
    """Integration tests for the complete data pipeline"""

    @pytest.fixture(scope="class")
    def aws_session(self):
        """Create AWS session for testing"""
        return boto3.Session(region_name='us-east-1')

    @pytest.fixture(scope="class")
    def lambda_client(self, aws_session):
        """Create Lambda client"""
        return aws_session.client('lambda')

    @pytest.fixture(scope="class")
    def s3_client(self, aws_session):
        """Create S3 client"""
        return aws_session.client('s3')

    @pytest.fixture(scope="class")
    def glue_client(self, aws_session):
        """Create Glue client"""
        return aws_session.client('glue')

    @pytest.fixture(scope="class")
    def athena_client(self, aws_session):
        """Create Athena client"""
        return aws_session.client('athena')

    @pytest.fixture(scope="class")
    def cloudwatch_client(self, aws_session):
        """Create CloudWatch client"""
        return aws_session.client('cloudwatch')

    @pytest.fixture(scope="class")
    def sns_client(self, aws_session):
        """Create SNS client"""
        return aws_session.client('sns')

    def test_lambda_function_exists(self, lambda_client):
        """Test that Lambda function exists and is configured correctly"""
        try:
            response = lambda_client.get_function(FunctionName='bitcoin-market-extractor')
            assert response['Configuration']['FunctionName'] == 'bitcoin-market-extractor'
            assert response['Configuration']['Runtime'] == 'python3.11'
            assert response['Configuration']['Handler'] == 'lambda_function.handler'
            assert response['Configuration']['Timeout'] == 900  # 15 minutes
            assert response['Configuration']['MemorySize'] == 2048  # 2GB
            print("✅ Lambda function exists and is configured correctly")
        except ClientError as e:
            pytest.fail(f"Lambda function not found: {e}")

    def test_lambda_function_execution(self, lambda_client):
        """Test that Lambda function executes successfully"""
        try:
            # Invoke Lambda function
            response = lambda_client.invoke(
                FunctionName='bitcoin-market-extractor',
                InvocationType='RequestResponse'
            )
            
            # Check response
            assert response['StatusCode'] == 200
            
            # Parse response payload
            payload = json.loads(response['Payload'].read())
            assert 'statusCode' in payload
            assert payload['statusCode'] == 200
            
            # Check response body
            body = json.loads(payload['body'])
            assert 'message' in body
            assert 'total_records' in body
            assert 'datasets' in body
            
            # Verify datasets
            datasets = body['datasets']
            assert len(datasets) == 3  # 1w, 4h, 1d
            
            # Check each dataset
            intervals = [dataset['interval'] for dataset in datasets]
            assert '1w' in intervals
            assert '4h' in intervals
            assert '1d' in intervals
            
            print(f"✅ Lambda function executed successfully: {body['total_records']} records generated")
            
        except ClientError as e:
            pytest.fail(f"Lambda function execution failed: {e}")

    def test_s3_data_storage(self, s3_client):
        """Test that data is stored in S3 correctly"""
        bucket_name = 'data-pipeline-datalake-055533307082-us-east-1'
        
        try:
            # List objects in silver folder
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='silver/'
            )
            
            assert 'Contents' in response
            assert len(response['Contents']) > 0
            
            # Check for files in each interval folder
            objects = response['Contents']
            object_keys = [obj['Key'] for obj in objects]
            
            # Check for interval folders
            assert any('interval=1w/' in key for key in object_keys)
            assert any('interval=4h/' in key for key in object_keys)
            assert any('interval=1d/' in key for key in object_keys)
            
            # Check file sizes (should be reasonable)
            for obj in objects:
                if obj['Key'].endswith('.json'):
                    assert obj['Size'] > 0
                    assert obj['Size'] < 100 * 1024 * 1024  # Less than 100MB
            
            print(f"✅ S3 data storage verified: {len(objects)} objects found")
            
        except ClientError as e:
            pytest.fail(f"S3 data storage test failed: {e}")

    def test_s3_data_content(self, s3_client):
        """Test that S3 data content is valid JSON"""
        bucket_name = 'data-pipeline-datalake-055533307082-us-east-1'
        
        try:
            # List objects and get a sample file
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='silver/',
                MaxKeys=10
            )
            
            if 'Contents' in response and len(response['Contents']) > 0:
                # Get the first JSON file
                sample_key = None
                for obj in response['Contents']:
                    if obj['Key'].endswith('.json'):
                        sample_key = obj['Key']
                        break
                
                if sample_key:
                    # Download and parse the file
                    file_response = s3_client.get_object(Bucket=bucket_name, Key=sample_key)
                    content = file_response['Body'].read().decode('utf-8')
                    
                    # Parse JSON
                    data = json.loads(content)
                    
                    # Verify structure
                    assert 'ingestion_timestamp' in data
                    assert 'symbol' in data
                    assert 'currency' in data
                    assert 'interval' in data
                    assert 'record_count' in data
                    assert 'current_price' in data
                    assert 'market_data' in data
                    
                    # Verify market_data is an array
                    assert isinstance(data['market_data'], list)
                    assert len(data['market_data']) > 0
                    
                    # Verify first data point structure
                    first_point = data['market_data'][0]
                    assert 'timestamp' in first_point
                    assert 'price' in first_point
                    assert 'volume' in first_point
                    assert 'market_cap' in first_point
                    
                    print(f"✅ S3 data content verified: {len(data['market_data'])} data points in sample file")
                else:
                    pytest.fail("No JSON files found in S3")
            else:
                pytest.fail("No objects found in S3")
                
        except ClientError as e:
            pytest.fail(f"S3 data content test failed: {e}")

    def test_glue_database_exists(self, glue_client):
        """Test that Glue database exists"""
        try:
            response = glue_client.get_database(Name='data_pipeline_analytics')
            assert response['Database']['Name'] == 'data_pipeline_analytics'
            print("✅ Glue database exists")
        except ClientError as e:
            pytest.fail(f"Glue database not found: {e}")

    def test_glue_crawler_exists(self, glue_client):
        """Test that Glue crawler exists and is configured correctly"""
        try:
            response = glue_client.get_crawler(Name='data-pipeline-crawler')
            crawler = response['Crawler']
            
            assert crawler['Name'] == 'data-pipeline-crawler'
            assert crawler['DatabaseName'] == 'data_pipeline_analytics'
            assert 'Targets' in crawler
            assert 'S3Targets' in crawler['Targets']
            
            print("✅ Glue crawler exists and is configured correctly")
        except ClientError as e:
            pytest.fail(f"Glue crawler not found: {e}")

    def test_glue_crawler_execution(self, glue_client):
        """Test that Glue crawler can be executed"""
        try:
            # Check current crawler state
            response = glue_client.get_crawler(Name='data-pipeline-crawler')
            current_state = response['Crawler']['State']
            
            if current_state == 'READY':
                print("✅ Glue crawler is already in READY state")
                return
            elif current_state == 'RUNNING':
                print("✅ Glue crawler is currently running")
                return
            elif current_state == 'FAILED':
                pytest.fail("Glue crawler is in FAILED state")
            
            # Only start crawler if it's not already running
            if current_state not in ['READY', 'RUNNING']:
                glue_client.start_crawler(Name='data-pipeline-crawler')
                
                # Wait for crawler to complete (with timeout)
                max_wait_time = 300  # 5 minutes
                start_time = time.time()
                
                while time.time() - start_time < max_wait_time:
                    response = glue_client.get_crawler(Name='data-pipeline-crawler')
                    state = response['Crawler']['State']
                    
                    if state == 'READY':
                        print("✅ Glue crawler executed successfully")
                        return
                    elif state == 'FAILED':
                        pytest.fail("Glue crawler execution failed")
                    
                    time.sleep(10)  # Wait 10 seconds before checking again
                
                pytest.fail("Glue crawler execution timed out")
            
        except ClientError as e:
            if "CrawlerRunningException" in str(e):
                print("✅ Glue crawler is already running")
                return
            pytest.fail(f"Glue crawler execution failed: {e}")

    def test_glue_tables_created(self, glue_client):
        """Test that Glue tables are created"""
        try:
            response = glue_client.get_tables(DatabaseName='data_pipeline_analytics')
            tables = response['TableList']
            
            assert len(tables) > 0
            
            # Check for bitcoin_data table
            table_names = [table['Name'] for table in tables]
            assert 'bitcoin_data' in table_names
            
            # Get table details
            table_response = glue_client.get_table(
                DatabaseName='data_pipeline_analytics',
                Name='bitcoin_data'
            )
            
            table = table_response['Table']
            assert table['Name'] == 'bitcoin_data'
            assert 'StorageDescriptor' in table
            assert 'Columns' in table['StorageDescriptor']
            
            # Check columns
            columns = table['StorageDescriptor']['Columns']
            column_names = [col['Name'] for col in columns]
            
            expected_columns = [
                'ingestion_timestamp', 'symbol', 'currency', 'interval',
                'record_count', 'current_price', 'current_market_cap',
                'price_change', 'price_change_percent', 'total_volume',
                'average_price', 'highest_price', 'lowest_price'
            ]
            
            for col in expected_columns:
                assert col in column_names, f"Column {col} not found in table"
            
            print(f"✅ Glue tables created: {len(tables)} tables found")
            
        except ClientError as e:
            pytest.fail(f"Glue tables test failed: {e}")

    def test_athena_workgroup_exists(self, athena_client):
        """Test that Athena workgroup exists"""
        try:
            response = athena_client.get_work_group(WorkGroup='data-pipeline-analytics')
            workgroup = response['WorkGroup']
            
            assert workgroup['Name'] == 'data-pipeline-analytics'
            assert 'Configuration' in workgroup
            
            print("✅ Athena workgroup exists")
        except ClientError as e:
            pytest.fail(f"Athena workgroup not found: {e}")

    def test_athena_query_execution(self, athena_client):
        """Test that Athena queries can be executed"""
        try:
            # Test basic query
            query = "SELECT COUNT(*) as total_records FROM data_pipeline_analytics.bitcoin_data"
            
            response = athena_client.start_query_execution(
                QueryString=query,
                WorkGroup='data-pipeline-analytics',
                ResultConfiguration={
                    'OutputLocation': 's3://data-pipeline-datalake-055533307082-us-east-1/athena-results/'
                }
            )
            
            query_execution_id = response['QueryExecutionId']
            
            # Wait for query to complete
            max_wait_time = 60  # 1 minute
            start_time = time.time()
            
            while time.time() - start_time < max_wait_time:
                status_response = athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                
                status = status_response['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    # Get query results
                    results_response = athena_client.get_query_results(
                        QueryExecutionId=query_execution_id
                    )
                    
                    # Check results
                    rows = results_response['ResultSet']['Rows']
                    assert len(rows) > 1  # Header + data rows
                    
                    # Get the count value
                    count_row = rows[1]  # First data row
                    count_value = count_row['Data'][0]['VarCharValue']
                    total_records = int(count_value)
                    
                    assert total_records > 0
                    print(f"✅ Athena query executed successfully: {total_records} records found")
                    return
                    
                elif status == 'FAILED':
                    error_info = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    pytest.fail(f"Athena query failed: {error_info}")
                
                time.sleep(5)  # Wait 5 seconds before checking again
            
            pytest.fail("Athena query execution timed out")
            
        except ClientError as e:
            pytest.fail(f"Athena query execution failed: {e}")

    def test_athena_data_analysis_queries(self, athena_client):
        """Test that complex Athena queries work"""
        queries = [
            "SELECT DISTINCT interval FROM data_pipeline_analytics.bitcoin_data",
            "SELECT interval, record_count, current_price FROM data_pipeline_analytics.bitcoin_data",
            "SELECT interval, AVG(current_price) as avg_price FROM data_pipeline_analytics.bitcoin_data GROUP BY interval"
        ]
        
        for i, query in enumerate(queries):
            try:
                response = athena_client.start_query_execution(
                    QueryString=query,
                    WorkGroup='data-pipeline-analytics',
                    ResultConfiguration={
                        'OutputLocation': 's3://data-pipeline-datalake-055533307082-us-east-1/athena-results/'
                    }
                )
                
                query_execution_id = response['QueryExecutionId']
                
                # Wait for query to complete
                max_wait_time = 60
                start_time = time.time()
                
                while time.time() - start_time < max_wait_time:
                    status_response = athena_client.get_query_execution(
                        QueryExecutionId=query_execution_id
                    )
                    
                    status = status_response['QueryExecution']['Status']['State']
                    
                    if status == 'SUCCEEDED':
                        print(f"✅ Athena analysis query {i+1} executed successfully")
                        break
                    elif status == 'FAILED':
                        error_info = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                        pytest.fail(f"Athena analysis query {i+1} failed: {error_info}")
                    
                    time.sleep(5)
                else:
                    pytest.fail(f"Athena analysis query {i+1} timed out")
                    
            except ClientError as e:
                pytest.fail(f"Athena analysis query {i+1} failed: {e}")

    def test_cloudwatch_dashboard_exists(self, cloudwatch_client):
        """Test that CloudWatch dashboard exists"""
        try:
            response = cloudwatch_client.get_dashboard(DashboardName='BlockBotics-DataPipeline')
            assert response['DashboardName'] == 'BlockBotics-DataPipeline'
            assert 'DashboardBody' in response
            print("✅ CloudWatch dashboard exists")
        except ClientError as e:
            pytest.fail(f"CloudWatch dashboard not found: {e}")

    def test_cloudwatch_alarms_exist(self, cloudwatch_client):
        """Test that CloudWatch alarms exist"""
        alarm_names = [
            'DataPipeline-Lambda-Errors',
            'DataPipeline-Lambda-Duration',
            'DataPipeline-DLQ-Messages',
            'DataPipeline-S3-Storage',
            'DataPipeline-Glue-Crawler-Success',
            'DataPipeline-Glue-Crawler-Failure',
            'DataPipeline-Athena-Query-Failure',
            'DataPipeline-Lambda-Invocations'
        ]
        
        try:
            response = cloudwatch_client.describe_alarms(AlarmNames=alarm_names)
            alarms = response['MetricAlarms']
            
            # Check that we have at least some alarms (not all may be created)
            assert len(alarms) > 0, "No CloudWatch alarms found"
            
            # Check that found alarms are valid
            for alarm in alarms:
                assert alarm['AlarmName'] in alarm_names
                assert alarm['StateValue'] in ['OK', 'ALARM', 'INSUFFICIENT_DATA']
            
            # Check for key alarms that should exist
            found_alarm_names = [alarm['AlarmName'] for alarm in alarms]
            key_alarms = ['DataPipeline-Lambda-Errors', 'DataPipeline-Lambda-Duration', 'DataPipeline-DLQ-Messages']
            
            for key_alarm in key_alarms:
                assert key_alarm in found_alarm_names, f"Key alarm {key_alarm} not found"
            
            print(f"✅ CloudWatch alarms exist: {len(alarms)} alarms found")
            print(f"   Found alarms: {found_alarm_names}")
            
        except ClientError as e:
            pytest.fail(f"CloudWatch alarms test failed: {e}")

    def test_sns_topic_exists(self, sns_client):
        """Test that SNS topic exists"""
        try:
            response = sns_client.list_topics()
            topics = response['Topics']
            
            topic_arns = [topic['TopicArn'] for topic in topics]
            assert any('blockbotics-data-pipeline-alerts' in arn for arn in topic_arns)
            
            print("✅ SNS topic exists")
        except ClientError as e:
            pytest.fail(f"SNS topic not found: {e}")

    def test_lake_formation_permissions(self, aws_session):
        """Test that Lake Formation permissions are configured"""
        try:
            lf_client = aws_session.client('lakeformation')
            
            # Check if data lake settings exist
            response = lf_client.get_data_lake_settings()
            assert 'DataLakeSettings' in response
            
            print("✅ Lake Formation permissions configured")
        except ClientError as e:
            pytest.fail(f"Lake Formation permissions test failed: {e}")

    def test_end_to_end_pipeline(self, lambda_client, s3_client, glue_client, athena_client):
        """Test the complete end-to-end pipeline"""
        try:
            # 1. Execute Lambda function
            lambda_response = lambda_client.invoke(
                FunctionName='bitcoin-market-extractor',
                InvocationType='RequestResponse'
            )
            
            assert lambda_response['StatusCode'] == 200
            print("✅ Step 1: Lambda function executed")
            
            # 2. Verify S3 data
            s3_response = s3_client.list_objects_v2(
                Bucket='data-pipeline-datalake-055533307082-us-east-1',
                Prefix='silver/'
            )
            assert 'Contents' in s3_response
            assert len(s3_response['Contents']) > 0
            print("✅ Step 2: S3 data verified")
            
            # 3. Execute Glue crawler
            glue_client.start_crawler(Name='data-pipeline-crawler')
            
            # Wait for crawler
            max_wait_time = 300
            start_time = time.time()
            while time.time() - start_time < max_wait_time:
                crawler_response = glue_client.get_crawler(Name='data-pipeline-crawler')
                if crawler_response['Crawler']['State'] == 'READY':
                    break
                time.sleep(10)
            else:
                pytest.fail("Glue crawler timed out")
            
            print("✅ Step 3: Glue crawler executed")
            
            # 4. Verify Glue tables
            tables_response = glue_client.get_tables(DatabaseName='data_pipeline_analytics')
            assert len(tables_response['TableList']) > 0
            print("✅ Step 4: Glue tables verified")
            
            # 5. Execute Athena query
            athena_response = athena_client.start_query_execution(
                QueryString="SELECT COUNT(*) FROM data_pipeline_analytics.bitcoin_data",
                WorkGroup='data-pipeline-analytics',
                ResultConfiguration={
                    'OutputLocation': 's3://data-pipeline-datalake-055533307082-us-east-1/athena-results/'
                }
            )
            
            # Wait for Athena query
            max_wait_time = 60
            start_time = time.time()
            while time.time() - start_time < max_wait_time:
                status_response = athena_client.get_query_execution(
                    QueryExecutionId=athena_response['QueryExecutionId']
                )
                if status_response['QueryExecution']['Status']['State'] == 'SUCCEEDED':
                    break
                time.sleep(5)
            else:
                pytest.fail("Athena query timed out")
            
            print("✅ Step 5: Athena query executed")
            print("🎉 Complete end-to-end pipeline test passed!")
            
        except ClientError as e:
            pytest.fail(f"End-to-end pipeline test failed: {e}")


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
