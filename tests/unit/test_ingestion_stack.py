"""
Unit tests for the Ingestion Stack
Tests Lambda function, IAM roles, SQS queue, and related resources
"""

import aws_cdk as core
import aws_cdk.assertions as assertions
import pytest

from stacks.ingestion_stack import BlockBoticsIngestion


class TestIngestionStack:
    """Test class for Ingestion Stack"""

    @pytest.fixture
    def app(self):
        """Create CDK app for testing"""
        return core.App()

    @pytest.fixture
    def data_lake_stack(self, app):
        """Create mock data lake stack"""
        from stacks.data_lake_stack import BlockBoticsDataLake
        return BlockBoticsDataLake(app, "test-data-lake-stack")

    @pytest.fixture
    def stack(self, app, data_lake_stack):
        """Create Ingestion stack for testing"""
        return BlockBoticsIngestion(app, "test-ingestion-stack", data_lake_stack)

    @pytest.fixture
    def template(self, stack):
        """Create CDK template for assertions"""
        return assertions.Template.from_stack(stack)

    def test_lambda_function_created(self, template):
        """Test that Lambda function is created with correct properties"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "bitcoin-market-extractor",
            "Runtime": "python3.11",
            "Handler": "lambda_function.handler",
            "Timeout": 900,  # 15 minutes
            "MemorySize": 2048,
            "Environment": {
                "Variables": {
                    "LOG_LEVEL": "INFO"
                }
            }
        })

    def test_lambda_function_has_correct_code(self, template):
        """Test that Lambda function has correct code configuration"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "Code": {
                "S3Bucket": assertions.Match.any_value(),
                "S3Key": assertions.Match.any_value()
            }
        })

    def test_lambda_function_has_layers(self, template):
        """Test that Lambda function has layers configured"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "Layers": [
                {
                    "Ref": assertions.Match.any_value()
                }
            ]
        })

    def test_lambda_function_has_dead_letter_queue(self, template):
        """Test that Lambda function has dead letter queue configured"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "DeadLetterConfig": {
                "TargetArn": {
                    "Ref": assertions.Match.any_value()
                }
            }
        })

    def test_lambda_role_created(self, template):
        """Test that Lambda IAM role is created"""
        template.has_resource_properties("AWS::IAM::Role", {
            "AssumeRolePolicyDocument": {
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "lambda.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
        })

    def test_lambda_role_has_basic_policies(self, template):
        """Test that Lambda role has basic execution policies"""
        template.has_resource_properties("AWS::IAM::Policy", {
            "PolicyDocument": {
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents"
                        ],
                        "Resource": "arn:aws:logs:*:*:*"
                    }
                ]
            }
        })

    def test_lambda_role_has_s3_permissions(self, template):
        """Test that Lambda role has S3 permissions"""
        template.has_resource_properties("AWS::IAM::Policy", {
            "PolicyDocument": {
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject"
                        ],
                        "Resource": assertions.Match.any_value()
                    }
                ]
            }
        })

    def test_lambda_role_has_kms_permissions(self, template):
        """Test that Lambda role has KMS permissions"""
        template.has_resource_properties("AWS::IAM::Policy", {
            "PolicyDocument": {
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "kms:Decrypt",
                            "kms:GenerateDataKey"
                        ],
                        "Resource": assertions.Match.any_value()
                    }
                ]
            }
        })

    def test_sqs_dlq_created(self, template):
        """Test that SQS dead letter queue is created"""
        template.has_resource_properties("AWS::SQS::Queue", {
            "QueueName": "blockbotics-ingestion-dlq",
            "MessageRetentionPeriod": 1209600  # 14 days
        })

    def test_sqs_dlq_has_encryption(self, template):
        """Test that SQS DLQ has encryption configured"""
        template.has_resource_properties("AWS::SQS::Queue", {
            "KmsMasterKeyId": {
                "Ref": assertions.Match.any_value()
            }
        })

    def test_lambda_layer_created(self, template):
        """Test that Lambda layer is created"""
        template.has_resource_properties("AWS::Lambda::LayerVersion", {
            "LayerName": "DependenciesLayer",
            "Description": "Dependencies layer for Bitcoin data extraction"
        })

    def test_lambda_layer_has_correct_code(self, template):
        """Test that Lambda layer has correct code configuration"""
        template.has_resource_properties("AWS::Lambda::LayerVersion", {
            "Content": {
                "S3Bucket": assertions.Match.any_value(),
                "S3Key": assertions.Match.any_value()
            }
        })

    def test_lambda_function_has_environment_variables(self, template):
        """Test that Lambda function has correct environment variables"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "Environment": {
                "Variables": {
                    "DATA_LAKE_BUCKET": {
                        "Ref": assertions.Match.any_value()
                    },
                    "KMS_KEY_ID": {
                        "Ref": assertions.Match.any_value()
                    },
                    "API_URL": "https://api.coinlore.net/api/ticker/?id=90",
                    "LOG_LEVEL": "INFO"
                }
            }
        })

    def test_lambda_function_has_vpc_config(self, template):
        """Test that Lambda function has VPC configuration"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "VpcConfig": {
                "SubnetIds": [],
                "SecurityGroupIds": []
            }
        })

    def test_lambda_function_has_tracing_config(self, template):
        """Test that Lambda function has tracing configuration"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "TracingConfig": {
                "Mode": "PassThrough"
            }
        })

    def test_lambda_function_has_ephemeral_storage(self, template):
        """Test that Lambda function has ephemeral storage configured"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "EphemeralStorage": {
                "Size": 512
            }
        })

    def test_lambda_function_has_snap_start(self, template):
        """Test that Lambda function has SnapStart configuration"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "SnapStart": {
                "ApplyOn": "None"
            }
        })

    def test_lambda_function_has_logging_config(self, template):
        """Test that Lambda function has logging configuration"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "LoggingConfig": {
                "LogFormat": "Text",
                "LogGroup": "/aws/lambda/bitcoin-market-extractor"
            }
        })

    def test_lambda_function_has_architecture(self, template):
        """Test that Lambda function has architecture specified"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "Architectures": ["x86_64"]
        })

    def test_lambda_function_has_package_type(self, template):
        """Test that Lambda function has package type specified"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "PackageType": "Zip"
        })

    def test_lambda_function_has_runtime_version_config(self, template):
        """Test that Lambda function has runtime version configuration"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "RuntimeVersionConfig": {
                "RuntimeVersionArn": assertions.Match.any_value()
            }
        })

    def test_lambda_function_has_correct_timeout(self, template):
        """Test that Lambda function has correct timeout for large datasets"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "Timeout": 900  # 15 minutes for generating 43,757 data points
        })

    def test_lambda_function_has_correct_memory(self, template):
        """Test that Lambda function has correct memory for large datasets"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "MemorySize": 2048  # 2GB for generating large datasets
        })

    def test_lambda_function_has_correct_handler(self, template):
        """Test that Lambda function has correct handler"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "Handler": "lambda_function.handler"
        })

    def test_lambda_function_has_correct_runtime(self, template):
        """Test that Lambda function has correct runtime"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "Runtime": "python3.11"
        })

    def test_lambda_function_has_correct_name(self, template):
        """Test that Lambda function has correct name"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "bitcoin-market-extractor"
        })

    def test_stack_has_correct_number_of_resources(self, template):
        """Test that stack has expected number of resources"""
        # Count different resource types
        lambda_functions = template.find_resources("AWS::Lambda::Function")
        lambda_layers = template.find_resources("AWS::Lambda::LayerVersion")
        sqs_queues = template.find_resources("AWS::SQS::Queue")
        iam_roles = template.find_resources("AWS::IAM::Role")
        iam_policies = template.find_resources("AWS::IAM::Policy")
        
        assert len(lambda_functions) == 1
        assert len(lambda_layers) == 1
        assert len(sqs_queues) == 1
        assert len(iam_roles) >= 1  # At least Lambda role
        assert len(iam_policies) >= 1  # At least one policy

    def test_lambda_function_has_all_required_permissions(self, template):
        """Test that Lambda function has all required permissions"""
        # Check for S3 permissions
        template.has_resource_properties("AWS::IAM::Policy", {
            "PolicyDocument": {
                "Statement": assertions.Match.array_with([
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject"
                        ],
                        "Resource": assertions.Match.any_value()
                    }
                ])
            }
        })

        # Check for KMS permissions
        template.has_resource_properties("AWS::IAM::Policy", {
            "PolicyDocument": {
                "Statement": assertions.Match.array_with([
                    {
                        "Effect": "Allow",
                        "Action": [
                            "kms:Decrypt",
                            "kms:GenerateDataKey"
                        ],
                        "Resource": assertions.Match.any_value()
                    }
                ])
            }
        })

    def test_lambda_function_has_correct_environment_variables(self, template):
        """Test that Lambda function has all required environment variables"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "Environment": {
                "Variables": {
                    "DATA_LAKE_BUCKET": assertions.Match.any_value(),
                    "KMS_KEY_ID": assertions.Match.any_value(),
                    "API_URL": "https://api.coinlore.net/api/ticker/?id=90",
                    "LOG_LEVEL": "INFO"
                }
            }
        })

    def test_stack_outputs(self, stack):
        """Test that stack has expected outputs"""
        outputs = stack.template.outputs
        assert "LambdaFunctionName" in outputs
        assert "LambdaFunctionArn" in outputs
        assert "DLQUrl" in outputs
        assert "LambdaLayerArn" in outputs

    def test_lambda_function_has_correct_description(self, template):
        """Test that Lambda function has correct description"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "Description": "Bitcoin market data extraction function"
        })

    def test_lambda_function_has_correct_tags(self, template):
        """Test that Lambda function has correct tags"""
        template.has_resource_properties("AWS::Lambda::Function", {
            "Tags": [
                {
                    "Key": "Project",
                    "Value": "BlockBotics"
                },
                {
                    "Key": "Environment",
                    "Value": "Production"
                }
            ]
        })
