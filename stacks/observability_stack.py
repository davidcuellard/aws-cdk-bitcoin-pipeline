#!/usr/bin/env python3
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    aws_sns as sns,
    aws_iam as iam,
    Duration,
    CfnOutput,
)
from constructs import Construct
from stacks.data_lake_stack import DataLakeStack
from stacks.ingestion_stack import IngestionStack


class ObservabilityStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        data_lake: DataLakeStack,
        ingestion: IngestionStack,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # SNS Topic for alerts
        self.alerts_topic = sns.Topic(
            self,
            "BlockBoticsAlerts",
            topic_name="blockbotics-data-pipeline-alerts",
            display_name="BlockBotics Data Pipeline Alerts",
        )

        # CloudWatch Dashboard
        self.dashboard = cloudwatch.Dashboard(
            self,
            "BlockBoticsDataPipelineDashboard",
            dashboard_name="BlockBotics-DataPipeline",
        )

        # Lambda Metrics Widgets
        lambda_metrics = cloudwatch.GraphWidget(
            title="Lambda Function Metrics",
            left=[
                cloudwatch.Metric(
                    namespace="AWS/Lambda",
                    metric_name="Duration",
                    dimensions_map={
                        "FunctionName": ingestion.data_extraction_lambda.function_name
                    },
                    statistic="Average",
                    period=Duration.minutes(5),
                ),
                cloudwatch.Metric(
                    namespace="AWS/Lambda",
                    metric_name="Errors",
                    dimensions_map={
                        "FunctionName": ingestion.data_extraction_lambda.function_name
                    },
                    statistic="Sum",
                    period=Duration.minutes(5),
                ),
                cloudwatch.Metric(
                    namespace="AWS/Lambda",
                    metric_name="Invocations",
                    dimensions_map={
                        "FunctionName": ingestion.data_extraction_lambda.function_name
                    },
                    statistic="Sum",
                    period=Duration.minutes(5),
                ),
            ],
            width=12,
            height=6,
        )

        # S3 Metrics Widget
        s3_metrics = cloudwatch.GraphWidget(
            title="S3 Data Lake Metrics",
            left=[
                cloudwatch.Metric(
                    namespace="AWS/S3",
                    metric_name="BucketSizeBytes",
                    dimensions_map={
                        "BucketName": data_lake.data_lake_bucket.bucket_name
                    },
                    statistic="Average",
                    period=Duration.hours(1),
                ),
                cloudwatch.Metric(
                    namespace="AWS/S3",
                    metric_name="NumberOfObjects",
                    dimensions_map={
                        "BucketName": data_lake.data_lake_bucket.bucket_name
                    },
                    statistic="Average",
                    period=Duration.hours(1),
                ),
            ],
            width=12,
            height=6,
        )

        # Athena Metrics Widget
        athena_metrics = cloudwatch.GraphWidget(
            title="Athena Query Metrics",
            left=[
                cloudwatch.Metric(
                    namespace="AWS/Athena",
                    metric_name="DataScannedInBytes",
                    dimensions_map={"WorkGroup": data_lake.athena_workgroup.name},
                    statistic="Sum",
                    period=Duration.hours(1),
                ),
                cloudwatch.Metric(
                    namespace="AWS/Athena",
                    metric_name="QueryStateChange",
                    dimensions_map={"WorkGroup": data_lake.athena_workgroup.name},
                    statistic="Sum",
                    period=Duration.hours(1),
                ),
            ],
            width=12,
            height=6,
        )

        # Add widgets to dashboard
        self.dashboard.add_widgets(lambda_metrics)
        self.dashboard.add_widgets(s3_metrics)
        self.dashboard.add_widgets(athena_metrics)

        # Lambda Error Alarm
        lambda_error_alarm = cloudwatch.Alarm(
            self,
            "LambdaErrorAlarm",
            alarm_name="DataPipeline-Lambda-Errors",
            metric=cloudwatch.Metric(
                namespace="AWS/Lambda",
                metric_name="Errors",
                dimensions_map={
                    "FunctionName": ingestion.data_extraction_lambda.function_name
                },
                statistic="Sum",
                period=Duration.minutes(5),
            ),
            threshold=1,
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        # Lambda Duration Alarm
        lambda_duration_alarm = cloudwatch.Alarm(
            self,
            "LambdaDurationAlarm",
            alarm_name="DataPipeline-Lambda-Duration",
            metric=cloudwatch.Metric(
                namespace="AWS/Lambda",
                metric_name="Duration",
                dimensions_map={
                    "FunctionName": ingestion.data_extraction_lambda.function_name
                },
                statistic="Average",
                period=Duration.minutes(5),
            ),
            threshold=300000,  # 5 minutes in milliseconds
            evaluation_periods=2,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        # DLQ Messages Alarm
        dlq_alarm = cloudwatch.Alarm(
            self,
            "DLQAlarm",
            alarm_name="DataPipeline-DLQ-Messages",
            metric=cloudwatch.Metric(
                namespace="AWS/SQS",
                metric_name="ApproximateNumberOfVisibleMessages",
                dimensions_map={"QueueName": ingestion.dlq.queue_name},
                statistic="Average",
                period=Duration.minutes(1),
            ),
            threshold=1,
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        # Add alarm actions
        lambda_error_alarm.add_alarm_action(cw_actions.SnsAction(self.alerts_topic))
        lambda_duration_alarm.add_alarm_action(cw_actions.SnsAction(self.alerts_topic))
        dlq_alarm.add_alarm_action(cw_actions.SnsAction(self.alerts_topic))

        # Cost monitoring (optional)
        cost_alarm = cloudwatch.Alarm(
            self,
            "DataPipelineCostAlarm",
            alarm_name="DataPipeline-Cost",
            metric=cloudwatch.Metric(
                namespace="AWS/Billing",
                metric_name="EstimatedCharges",
                dimensions_map={"Currency": "USD"},
                statistic="Maximum",
                period=Duration.days(1),
            ),
            threshold=50,  # $50 threshold
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        # Outputs
        CfnOutput(
            self,
            "DashboardUrl",
            value=f"https://{self.region}.console.aws.amazon.com/cloudwatch/home?region={self.region}#dashboards:name={self.dashboard.dashboard_name}",
            description="CloudWatch Dashboard URL",
        )

        CfnOutput(
            self,
            "AlertsTopicArn",
            value=self.alerts_topic.topic_arn,
            description="SNS Topic ARN for alerts",
        )
