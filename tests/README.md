# Bitcoin Data Pipeline Test Suite

This directory contains comprehensive tests for the Bitcoin data pipeline built with AWS CDK.

## Test Structure

```
tests/
├── unit/                          # Unit tests for CDK stacks
│   ├── test_aws_cdk_stack.py     # Main app integration tests
│   ├── test_data_lake_stack.py   # Data lake stack tests
│   ├── test_ingestion_stack.py   # Ingestion stack tests
│   └── test_observability_stack.py # Observability stack tests
├── integration/                   # Integration tests
│   ├── test_pipeline_integration.py # End-to-end pipeline tests
│   ├── test_data_quality.py      # Data quality tests
│   └── test_performance.py       # Performance tests
├── run_tests.py                  # Test runner script
├── requirements-test.txt         # Test dependencies
└── README.md                     # This file
```

## Test Categories

### 1. Unit Tests (`tests/unit/`)

- **Purpose**: Test individual CDK stacks and resources
- **Scope**: CDK template generation, resource properties, stack outputs
- **Dependencies**: CDK libraries, pytest
- **Run Time**: Fast (< 30 seconds)

### 2. Integration Tests (`tests/integration/`)

- **Purpose**: Test actual AWS service interactions
- **Scope**: Lambda execution, S3 operations, Glue crawler, Athena queries
- **Dependencies**: AWS credentials, deployed infrastructure
- **Run Time**: Medium (2-5 minutes)

### 3. Data Quality Tests (`tests/integration/test_data_quality.py`)

- **Purpose**: Validate data accuracy, completeness, and consistency
- **Scope**: JSON structure, data types, business logic validation
- **Dependencies**: S3 access, data files
- **Run Time**: Fast (< 1 minute)

### 4. Performance Tests (`tests/integration/test_performance.py`)

- **Purpose**: Validate execution time, resource usage, scalability
- **Scope**: Lambda duration, S3 upload speed, Athena query performance
- **Dependencies**: AWS services, CloudWatch metrics
- **Run Time**: Medium (3-10 minutes)

## Running Tests

### Prerequisites

1. **Install test dependencies**:

   ```bash
   pip install -r tests/requirements-test.txt
   ```

2. **AWS Credentials**: Ensure AWS credentials are configured

   ```bash
   aws configure list
   ```

3. **Deployed Infrastructure**: All CDK stacks must be deployed
   ```bash
   cdk deploy --all
   ```

### Quick Test Run

```bash
# Run all tests
python tests/run_tests.py

# Run specific test categories
pytest tests/unit/ -v                    # Unit tests only
pytest tests/integration/ -v            # Integration tests only
pytest tests/integration/test_data_quality.py -v  # Data quality only
```

### Individual Test Execution

```bash
# Run specific test files
pytest tests/unit/test_data_lake_stack.py -v
pytest tests/integration/test_pipeline_integration.py -v

# Run specific test methods
pytest tests/integration/test_pipeline_integration.py::TestPipelineIntegration::test_lambda_function_execution -v

# Run with coverage
pytest tests/ --cov=stacks --cov-report=html
```

## Test Descriptions

### Unit Tests

#### `test_aws_cdk_stack.py`

- Tests main application stack integration
- Validates all sub-stacks are created
- Checks resource properties and outputs

#### `test_data_lake_stack.py`

- Tests S3 bucket configuration
- Validates Glue database and crawler setup
- Checks Athena workgroup configuration
- Verifies Lake Formation permissions

#### `test_ingestion_stack.py`

- Tests Lambda function configuration
- Validates IAM roles and policies
- Checks SQS dead letter queue setup
- Verifies environment variables

#### `test_observability_stack.py`

- Tests CloudWatch dashboard creation
- Validates alarm configurations
- Checks SNS topic setup
- Verifies monitoring metrics

### Integration Tests

#### `test_pipeline_integration.py`

- **Lambda Function Tests**:

  - Function exists and is configured correctly
  - Function executes successfully
  - Response format is valid

- **S3 Storage Tests**:

  - Data is stored in correct location
  - File structure is correct
  - JSON content is valid

- **Glue Crawler Tests**:

  - Crawler exists and is configured
  - Crawler executes successfully
  - Tables are created correctly

- **Athena Query Tests**:

  - Workgroup exists
  - Queries execute successfully
  - Results are returned correctly

- **End-to-End Pipeline Test**:
  - Complete flow from Lambda to Athena
  - All components work together
  - Data flows correctly through pipeline

#### `test_data_quality.py`

- **Data Completeness**:

  - All required intervals present (1w, 4h, 1d)
  - Files exist for each interval
  - Data volume is appropriate

- **Data Structure**:

  - JSON format is valid
  - Required fields are present
  - Data types are correct

- **Data Accuracy**:

  - Bitcoin symbol is correct (BTC)
  - Currency is USD
  - Price values are positive and realistic

- **Data Consistency**:
  - Same symbol/currency across intervals
  - Consistent data structure
  - Valid timestamps

#### `test_performance.py`

- **Lambda Performance**:

  - Execution time < 10 minutes
  - Memory usage < 80%
  - No errors or throttles

- **S3 Performance**:

  - Upload time is reasonable
  - File sizes are appropriate
  - No empty or oversized files

- **Glue Crawler Performance**:

  - Execution time < 10 minutes
  - Successfully creates tables
  - No failures

- **Athena Performance**:
  - Query execution < 2 minutes
  - Results are returned
  - No query failures

## Test Results Interpretation

### Success Indicators

- ✅ All tests pass
- ✅ No errors in CloudWatch logs
- ✅ Data is properly formatted and complete
- ✅ Performance metrics are within acceptable ranges

### Failure Indicators

- ❌ Tests fail with specific error messages
- ❌ AWS service errors (permissions, resource not found)
- ❌ Data quality issues (missing fields, invalid format)
- ❌ Performance issues (timeouts, resource limits)

### Common Issues and Solutions

1. **AWS Credentials Issues**:

   ```bash
   aws configure list
   aws sts get-caller-identity
   ```

2. **Resource Not Found**:

   - Ensure CDK stacks are deployed
   - Check resource names match exactly
   - Verify region is correct

3. **Permission Issues**:

   - Check IAM policies
   - Verify Lake Formation permissions
   - Ensure service roles have correct permissions

4. **Data Quality Issues**:
   - Check Lambda function logs
   - Verify JSON format
   - Ensure data generation logic is correct

## Continuous Integration

### GitHub Actions Example

```yaml
name: Test Pipeline
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: "3.11"
      - run: pip install -r tests/requirements-test.txt
      - run: python tests/run_tests.py
```

### Local Development

```bash
# Run tests during development
pytest tests/unit/ -v --tb=short

# Run integration tests (requires deployed infrastructure)
pytest tests/integration/ -v

# Run specific failing test
pytest tests/integration/test_pipeline_integration.py::TestPipelineIntegration::test_lambda_function_execution -v -s
```

## Test Maintenance

### Adding New Tests

1. Create test file in appropriate directory
2. Follow naming convention: `test_*.py`
3. Use descriptive test names
4. Add proper docstrings
5. Include both positive and negative test cases

### Updating Tests

1. Update test when infrastructure changes
2. Maintain backward compatibility
3. Update documentation
4. Verify all tests still pass

### Test Data Management

1. Use realistic test data
2. Clean up test artifacts
3. Avoid hardcoded values
4. Use environment variables for configuration

## Troubleshooting

### Debug Mode

```bash
# Run with verbose output
pytest tests/ -v -s --tb=long

# Run single test with debug
pytest tests/integration/test_pipeline_integration.py::TestPipelineIntegration::test_lambda_function_execution -v -s --tb=long
```

### Log Analysis

```bash
# Check Lambda logs
aws logs describe-log-groups --log-group-name-prefix /aws/lambda/bitcoin-market-extractor

# Check CloudWatch metrics
aws cloudwatch get-metric-statistics --namespace AWS/Lambda --metric-name Duration --start-time 2024-01-01T00:00:00Z --end-time 2024-01-01T23:59:59Z --period 3600 --statistics Average
```

This comprehensive test suite ensures your Bitcoin data pipeline is working correctly and meets all requirements for the technical test! 🚀
