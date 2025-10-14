"""
Unit tests for the Observability Stack
Tests CloudWatch Dashboard, Alarms, and SNS notifications
"""

import aws_cdk as core
import aws_cdk.assertions as assertions
import pytest

from stacks.observability_stack import BlockBoticsObservability


class TestObservabilityStack:
    """Test class for Observability Stack"""

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
    def ingestion_stack(self, app, data_lake_stack):
        """Create mock ingestion stack"""
        from stacks.ingestion_stack import BlockBoticsIngestion
        return BlockBoticsIngestion(app, "test-ingestion-stack", data_lake_stack)

    @pytest.fixture
    def stack(self, app, data_lake_stack, ingestion_stack):
        """Create Observability stack for testing"""
        return BlockBoticsObservability(app, "test-observability-stack", data_lake_stack, ingestion_stack)

    @pytest.fixture
    def template(self, stack):
        """Create CDK template for assertions"""
        return assertions.Template.from_stack(stack)

    def test_cloudwatch_dashboard_created(self, template):
        """Test that CloudWatch Dashboard is created"""
        template.has_resource_properties("AWS::CloudWatch::Dashboard", {
            "DashboardName": "BlockBotics-DataPipeline"
        })

    def test_cloudwatch_dashboard_has_body(self, template):
        """Test that CloudWatch Dashboard has body content"""
        template.has_resource_properties("AWS::CloudWatch::Dashboard", {
            "DashboardBody": assertions.Match.any_value()
        })

    def test_sns_topic_created(self, template):
        """Test that SNS topic is created"""
        template.has_resource_properties("AWS::SNS::Topic", {
            "TopicName": "blockbotics-data-pipeline-alerts"
        })

    def test_sns_topic_has_encryption(self, template):
        """Test that SNS topic has encryption configured"""
        template.has_resource_properties("AWS::SNS::Topic", {
            "KmsMasterKeyId": {
                "Ref": assertions.Match.any_value()
            }
        })

    def test_lambda_error_alarm_created(self, template):
        """Test that Lambda error alarm is created"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "AlarmName": "DataPipeline-Lambda-Errors",
            "MetricName": "Errors",
            "Namespace": "AWS/Lambda",
            "Threshold": 1,
            "EvaluationPeriods": 1
        })

    def test_lambda_duration_alarm_created(self, template):
        """Test that Lambda duration alarm is created"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "AlarmName": "DataPipeline-Lambda-Duration",
            "MetricName": "Duration",
            "Namespace": "AWS/Lambda",
            "Threshold": 600000,  # 10 minutes in milliseconds
            "EvaluationPeriods": 1
        })

    def test_dlq_alarm_created(self, template):
        """Test that DLQ alarm is created"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "AlarmName": "DataPipeline-DLQ-Messages",
            "MetricName": "ApproximateNumberOfVisibleMessages",
            "Namespace": "AWS/SQS",
            "Threshold": 1,
            "EvaluationPeriods": 1
        })

    def test_s3_storage_alarm_created(self, template):
        """Test that S3 storage alarm is created"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "AlarmName": "DataPipeline-S3-Storage",
            "MetricName": "BucketSizeBytes",
            "Namespace": "AWS/S3",
            "Threshold": 1000000000,  # 1GB
            "EvaluationPeriods": 1
        })

    def test_glue_crawler_success_alarm_created(self, template):
        """Test that Glue crawler success alarm is created"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "AlarmName": "DataPipeline-Glue-Crawler-Success",
            "MetricName": "Success",
            "Namespace": "AWS/Glue",
            "Threshold": 1,
            "EvaluationPeriods": 1
        })

    def test_glue_crawler_failure_alarm_created(self, template):
        """Test that Glue crawler failure alarm is created"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "AlarmName": "DataPipeline-Glue-Crawler-Failure",
            "MetricName": "Failure",
            "Namespace": "AWS/Glue",
            "Threshold": 1,
            "EvaluationPeriods": 1
        })

    def test_athena_query_failure_alarm_created(self, template):
        """Test that Athena query failure alarm is created"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "AlarmName": "DataPipeline-Athena-Query-Failure",
            "MetricName": "QueryFailure",
            "Namespace": "AWS/Athena",
            "Threshold": 1,
            "EvaluationPeriods": 1
        })

    def test_lambda_invocations_alarm_created(self, template):
        """Test that Lambda invocations alarm is created"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "AlarmName": "DataPipeline-Lambda-Invocations",
            "MetricName": "Invocations",
            "Namespace": "AWS/Lambda",
            "Threshold": 10,
            "EvaluationPeriods": 1
        })

    def test_lambda_throttles_alarm_created(self, template):
        """Test that Lambda throttles alarm is created"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "AlarmName": "DataPipeline-Lambda-Throttles",
            "MetricName": "Throttles",
            "Namespace": "AWS/Lambda",
            "Threshold": 1,
            "EvaluationPeriods": 1
        })

    def test_lambda_concurrent_executions_alarm_created(self, template):
        """Test that Lambda concurrent executions alarm is created"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "AlarmName": "DataPipeline-Lambda-ConcurrentExecutions",
            "MetricName": "ConcurrentExecutions",
            "Namespace": "AWS/Lambda",
            "Threshold": 5,
            "EvaluationPeriods": 1
        })

    def test_alarms_have_sns_actions(self, template):
        """Test that alarms have SNS actions configured"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "AlarmActions": [
                {
                    "Ref": assertions.Match.any_value()
                }
            ]
        })

    def test_alarms_have_ok_actions(self, template):
        """Test that alarms have OK actions configured"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "OKActions": [
                {
                    "Ref": assertions.Match.any_value()
                }
            ]
        })

    def test_alarms_have_insufficient_data_actions(self, template):
        """Test that alarms have insufficient data actions configured"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "InsufficientDataActions": [
                {
                    "Ref": assertions.Match.any_value()
                }
            ]
        })

    def test_alarms_have_correct_comparison_operator(self, template):
        """Test that alarms have correct comparison operator"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "ComparisonOperator": "GreaterThanOrEqualToThreshold"
        })

    def test_alarms_have_correct_statistic(self, template):
        """Test that alarms have correct statistic"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "Statistic": "Sum"
        })

    def test_alarms_have_correct_period(self, template):
        """Test that alarms have correct period"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "Period": 300  # 5 minutes
        })

    def test_alarms_have_correct_treat_missing_data(self, template):
        """Test that alarms have correct treat missing data"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "TreatMissingData": "notBreaching"
        })

    def test_alarms_have_correct_alarm_description(self, template):
        """Test that alarms have correct alarm description"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "AlarmDescription": assertions.Match.any_value()
        })

    def test_alarms_have_correct_alarm_actions_enabled(self, template):
        """Test that alarms have correct alarm actions enabled"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "AlarmActionsEnabled": True
        })

    def test_alarms_have_correct_ok_actions_enabled(self, template):
        """Test that alarms have correct OK actions enabled"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "OKActionsEnabled": True
        })

    def test_alarms_have_correct_insufficient_data_actions_enabled(self, template):
        """Test that alarms have correct insufficient data actions enabled"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "InsufficientDataActionsEnabled": True
        })

    def test_stack_has_correct_number_of_resources(self, template):
        """Test that stack has expected number of resources"""
        # Count different resource types
        cloudwatch_dashboards = template.find_resources("AWS::CloudWatch::Dashboard")
        cloudwatch_alarms = template.find_resources("AWS::CloudWatch::Alarm")
        sns_topics = template.find_resources("AWS::SNS::Topic")
        
        assert len(cloudwatch_dashboards) == 1
        assert len(cloudwatch_alarms) >= 8  # At least 8 alarms
        assert len(sns_topics) == 1

    def test_dashboard_has_correct_widgets(self, template):
        """Test that dashboard has correct widgets"""
        template.has_resource_properties("AWS::CloudWatch::Dashboard", {
            "DashboardBody": assertions.Match.string_like_regexp(".*Lambda.*")
        })

    def test_dashboard_has_lambda_widgets(self, template):
        """Test that dashboard has Lambda widgets"""
        template.has_resource_properties("AWS::CloudWatch::Dashboard", {
            "DashboardBody": assertions.Match.string_like_regexp(".*bitcoin-market-extractor.*")
        })

    def test_dashboard_has_s3_widgets(self, template):
        """Test that dashboard has S3 widgets"""
        template.has_resource_properties("AWS::CloudWatch::Dashboard", {
            "DashboardBody": assertions.Match.string_like_regexp(".*S3.*")
        })

    def test_dashboard_has_glue_widgets(self, template):
        """Test that dashboard has Glue widgets"""
        template.has_resource_properties("AWS::CloudWatch::Dashboard", {
            "DashboardBody": assertions.Match.string_like_regexp(".*Glue.*")
        })

    def test_dashboard_has_athena_widgets(self, template):
        """Test that dashboard has Athena widgets"""
        template.has_resource_properties("AWS::CloudWatch::Dashboard", {
            "DashboardBody": assertions.Match.string_like_regexp(".*Athena.*")
        })

    def test_sns_topic_has_correct_name(self, template):
        """Test that SNS topic has correct name"""
        template.has_resource_properties("AWS::SNS::Topic", {
            "TopicName": "blockbotics-data-pipeline-alerts"
        })

    def test_sns_topic_has_correct_display_name(self, template):
        """Test that SNS topic has correct display name"""
        template.has_resource_properties("AWS::SNS::Topic", {
            "DisplayName": "BlockBotics Data Pipeline Alerts"
        })

    def test_sns_topic_has_correct_tags(self, template):
        """Test that SNS topic has correct tags"""
        template.has_resource_properties("AWS::SNS::Topic", {
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

    def test_stack_outputs(self, stack):
        """Test that stack has expected outputs"""
        outputs = stack.template.outputs
        assert "DashboardName" in outputs
        assert "SNSTopicArn" in outputs
        assert "AlarmCount" in outputs

    def test_alarms_have_correct_dimensions(self, template):
        """Test that alarms have correct dimensions"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "Dimensions": [
                {
                    "Name": "FunctionName",
                    "Value": "bitcoin-market-extractor"
                }
            ]
        })

    def test_alarms_have_correct_metric_name(self, template):
        """Test that alarms have correct metric name"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "MetricName": assertions.Match.one_of(
                "Errors",
                "Duration",
                "Invocations",
                "Throttles",
                "ConcurrentExecutions"
            )
        })

    def test_alarms_have_correct_namespace(self, template):
        """Test that alarms have correct namespace"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "Namespace": assertions.Match.one_of(
                "AWS/Lambda",
                "AWS/SQS",
                "AWS/S3",
                "AWS/Glue",
                "AWS/Athena"
            )
        })

    def test_alarms_have_correct_threshold(self, template):
        """Test that alarms have correct threshold"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "Threshold": assertions.Match.any_value()
        })

    def test_alarms_have_correct_evaluation_periods(self, template):
        """Test that alarms have correct evaluation periods"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "EvaluationPeriods": 1
        })

    def test_alarms_have_correct_period(self, template):
        """Test that alarms have correct period"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "Period": 300  # 5 minutes
        })

    def test_alarms_have_correct_statistic(self, template):
        """Test that alarms have correct statistic"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "Statistic": "Sum"
        })

    def test_alarms_have_correct_comparison_operator(self, template):
        """Test that alarms have correct comparison operator"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "ComparisonOperator": "GreaterThanOrEqualToThreshold"
        })

    def test_alarms_have_correct_treat_missing_data(self, template):
        """Test that alarms have correct treat missing data"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "TreatMissingData": "notBreaching"
        })

    def test_alarms_have_correct_alarm_actions_enabled(self, template):
        """Test that alarms have correct alarm actions enabled"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "AlarmActionsEnabled": True
        })

    def test_alarms_have_correct_ok_actions_enabled(self, template):
        """Test that alarms have correct OK actions enabled"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "OKActionsEnabled": True
        })

    def test_alarms_have_correct_insufficient_data_actions_enabled(self, template):
        """Test that alarms have correct insufficient data actions enabled"""
        template.has_resource_properties("AWS::CloudWatch::Alarm", {
            "InsufficientDataActionsEnabled": True
        })
