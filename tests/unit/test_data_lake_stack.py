"""
Unit tests for the Data Lake Stack
Tests S3 bucket, Glue database, Athena workgroup, and Lake Formation resources
"""

import aws_cdk as core
import aws_cdk.assertions as assertions
import pytest

from stacks.data_lake_stack import BlockBoticsDataLake


class TestDataLakeStack:
    """Test class for Data Lake Stack"""

    @pytest.fixture
    def app(self):
        """Create CDK app for testing"""
        return core.App()

    @pytest.fixture
    def stack(self, app):
        """Create Data Lake stack for testing"""
        return BlockBoticsDataLake(app, "test-data-lake-stack")

    @pytest.fixture
    def template(self, stack):
        """Create CDK template for assertions"""
        return assertions.Template.from_stack(stack)

    def test_s3_bucket_created(self, template):
        """Test that S3 bucket is created with correct properties"""
        template.has_resource_properties("AWS::S3::Bucket", {
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": [
                    {
                        "ServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "aws:kms"
                        }
                    }
                ]
            },
            "VersioningConfiguration": {
                "Status": "Enabled"
            },
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True
            }
        })

    def test_s3_bucket_lifecycle_rules(self, template):
        """Test that S3 bucket has lifecycle rules configured"""
        template.has_resource_properties("AWS::S3::Bucket", {
            "LifecycleConfiguration": {
                "Rules": [
                    {
                        "Id": "TransitionToIA",
                        "Status": "Enabled",
                        "Transitions": [
                            {
                                "StorageClass": "STANDARD_IA",
                                "TransitionInDays": 30
                            },
                            {
                                "StorageClass": "GLACIER",
                                "TransitionInDays": 90
                            }
                        ]
                    }
                ]
            }
        })

    def test_kms_key_created(self, template):
        """Test that KMS key is created"""
        template.has_resource_properties("AWS::KMS::Key", {
            "Description": "KMS key for data lake encryption",
            "KeyPolicy": {
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "AWS": {
                                "Fn::Sub": "arn:aws:iam::${AWS::AccountId}:root"
                            }
                        },
                        "Action": "kms:*",
                        "Resource": "*"
                    }
                ]
            }
        })

    def test_glue_database_created(self, template):
        """Test that Glue database is created"""
        template.has_resource_properties("AWS::Glue::Database", {
            "DatabaseInput": {
                "Name": "data_pipeline_analytics",
                "Description": "Data pipeline analytics database"
            }
        })

    def test_glue_crawler_created(self, template):
        """Test that Glue crawler is created"""
        template.has_resource_properties("AWS::Glue::Crawler", {
            "Name": "data-pipeline-crawler",
            "DatabaseName": {
                "Ref": assertions.Match.any_value()
            }
        })

    def test_glue_crawler_role_created(self, template):
        """Test that Glue crawler IAM role is created"""
        template.has_resource_properties("AWS::IAM::Role", {
            "AssumeRolePolicyDocument": {
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "glue.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
        })

    def test_athena_workgroup_created(self, template):
        """Test that Athena workgroup is created"""
        template.has_resource_properties("AWS::Athena::WorkGroup", {
            "Name": "data-pipeline-analytics",
            "WorkGroupConfiguration": {
                "ResultConfiguration": {
                    "OutputLocation": {
                        "Fn::Sub": "s3://${DataLakeBucket}/athena-results/"
                    },
                    "EncryptionConfiguration": {
                        "EncryptionOption": "SSE_KMS"
                    }
                }
            }
        })

    def test_lake_formation_admin_created(self, template):
        """Test that Lake Formation admin is configured"""
        template.has_resource_properties("AWS::LakeFormation::DataLakeSettings", {
            "Admins": [
                {
                    "DataLakePrincipalIdentifier": {
                        "Fn::Sub": "arn:aws:iam::${AWS::AccountId}:root"
                    }
                }
            ]
        })

    def test_lake_formation_resource_created(self, template):
        """Test that Lake Formation resource is created"""
        template.has_resource_properties("AWS::LakeFormation::Resource", {
            "ResourceArn": {
                "Fn::GetAtt": [
                    assertions.Match.any_value(),
                    "Arn"
                ]
            },
            "UseServiceLinkedRole": True
        })

    def test_s3_bucket_policy_created(self, template):
        """Test that S3 bucket policy is created for Lake Formation"""
        template.has_resource_properties("AWS::S3::BucketPolicy", {
            "PolicyDocument": {
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "lakeformation.amazonaws.com"
                        },
                        "Action": "s3:GetObject",
                        "Resource": {
                            "Fn::Sub": "arn:aws:s3:::${DataLakeBucket}/*"
                        }
                    }
                ]
            }
        })

    def test_glue_crawler_s3_targets(self, template):
        """Test that Glue crawler has correct S3 targets"""
        template.has_resource_properties("AWS::Glue::Crawler", {
            "Targets": {
                "S3Targets": [
                    {
                        "Path": {
                            "Fn::Sub": "s3://${DataLakeBucket}/silver/"
                        }
                    }
                ]
            }
        })

    def test_athena_workgroup_enforce_configuration(self, template):
        """Test that Athena workgroup enforces configuration"""
        template.has_resource_properties("AWS::Athena::WorkGroup", {
            "WorkGroupConfiguration": {
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True
            }
        })

    def test_kms_key_alias_created(self, template):
        """Test that KMS key alias is created"""
        template.has_resource_properties("AWS::KMS::Alias", {
            "AliasName": "alias/data-pipeline-datalake-key"
        })

    def test_glue_crawler_schedule(self, template):
        """Test that Glue crawler has schedule configuration"""
        template.has_resource_properties("AWS::Glue::Crawler", {
            "Schedule": {
                "ScheduleExpression": "cron(0 2 * * ? *)"
            }
        })

    def test_s3_bucket_notification_created(self, template):
        """Test that S3 bucket notification is created for Lambda trigger"""
        template.has_resource_properties("AWS::S3::Bucket", {
            "NotificationConfiguration": {
                "LambdaConfigurations": [
                    {
                        "Event": "s3:ObjectCreated:*",
                        "Function": {
                            "Ref": assertions.Match.any_value()
                        }
                    }
                ]
            }
        })

    def test_all_resources_have_correct_names(self, template):
        """Test that all resources have expected names"""
        # Check for specific resource names
        template.has_resource("AWS::S3::Bucket", {
            "Properties": {
                "BucketName": {
                    "Fn::Sub": "data-pipeline-datalake-${AWS::AccountId}-${AWS::Region}"
                }
            }
        })

    def test_lake_formation_permissions_created(self, template):
        """Test that Lake Formation permissions are created"""
        template.has_resource_properties("AWS::LakeFormation::PrincipalPermissions", {
            "Principal": {
                "DataLakePrincipalIdentifier": {
                    "Fn::Sub": "arn:aws:iam::${AWS::AccountId}:root"
                }
            },
            "Permissions": ["ALL"],
            "PermissionsWithGrantOption": ["ALL"]
        })

    def test_stack_outputs(self, stack):
        """Test that stack has expected outputs"""
        outputs = stack.template.outputs
        assert "DataLakeBucketName" in outputs
        assert "GlueDatabaseName" in outputs
        assert "AthenaWorkGroupName" in outputs
        assert "KMSKeyId" in outputs

    def test_stack_has_correct_number_of_resources(self, template):
        """Test that stack has expected number of resources"""
        # Count different resource types
        s3_buckets = template.find_resources("AWS::S3::Bucket")
        kms_keys = template.find_resources("AWS::KMS::Key")
        glue_databases = template.find_resources("AWS::Glue::Database")
        glue_crawlers = template.find_resources("AWS::Glue::Crawler")
        athena_workgroups = template.find_resources("AWS::Athena::WorkGroup")
        
        assert len(s3_buckets) == 1
        assert len(kms_keys) == 1
        assert len(glue_databases) == 1
        assert len(glue_crawlers) == 1
        assert len(athena_workgroups) == 1

    def test_glue_crawler_has_correct_configuration(self, template):
        """Test that Glue crawler has all required configuration"""
        template.has_resource_properties("AWS::Glue::Crawler", {
            "Configuration": {
                "Json": {
                    "Fn::Sub": '{"Version": 1.0, "Grouping": {"TableGroupingPolicy": "CombineCompatibleSchemas"}}'
                }
            },
            "SchemaChangePolicy": {
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "DEPRECATE_IN_DATABASE"
            }
        })

    def test_athena_workgroup_has_engine_version(self, template):
        """Test that Athena workgroup has engine version configured"""
        template.has_resource_properties("AWS::Athena::WorkGroup", {
            "WorkGroupConfiguration": {
                "EngineVersion": {
                    "SelectedEngineVersion": "AUTO"
                }
            }
        })
