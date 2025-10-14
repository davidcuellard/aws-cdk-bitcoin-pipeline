#!/usr/bin/env python3
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_sqs as sqs,
    aws_logs as logs,
    aws_lakeformation as lakeformation,
    Duration,
    CfnOutput,
)
from constructs import Construct
from stacks.data_lake_stack import DataLakeStack


class IngestionStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, data_lake: DataLakeStack, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Dead Letter Queue for failed Lambda invocations
        self.dlq = sqs.Queue(
            self,
            "IngestionDLQ",
            queue_name="blockbotics-ingestion-dlq",
            retention_period=Duration.days(14),
        )

        # Lambda execution role
        self.lambda_role = iam.Role(
            self,
            "ModelZIngestionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
            inline_policies={
                "S3Access": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:PutObject",
                                "s3:PutObjectAcl",
                                "s3:GetObject",
                                "s3:DeleteObject",
                            ],
                            resources=[f"{data_lake.data_lake_bucket.bucket_arn}/*"],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["s3:ListBucket"],
                            resources=[data_lake.data_lake_bucket.bucket_arn],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["kms:Decrypt", "kms:GenerateDataKey"],
                            resources=[data_lake.kms_key.key_arn],
                        ),
                    ]
                )
            },
        )

        # Lake Formation permissions for Lambda role (removed due to access issues)

        # Lambda Layer for dependencies
        self.lambda_layer = lambda_.LayerVersion(
            self, "DependenciesLayer",
            code=lambda_.Code.from_asset("lambda-layer"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_11],
            description="Dependencies for data extraction Lambda"
        )

        # Lambda function for Bitcoin market data extraction
        self.data_extraction_lambda = lambda_.Function(
            self,
            "DataExtractionLambda",
            function_name="bitcoin-market-extractor",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="lambda_function.handler",
            code=lambda_.Code.from_asset("lambda"),
            layers=[self.lambda_layer],
            role=self.lambda_role,
            timeout=Duration.minutes(15),  # Increased timeout for large datasets
            memory_size=2048,  # Increased memory for processing large datasets
            dead_letter_queue=self.dlq,
            environment={
                "DATA_LAKE_BUCKET": data_lake.data_lake_bucket.bucket_name,
            },
            log_retention=logs.RetentionDays.ONE_MONTH,
        )

        # EventBridge schedules for incremental updates
        # 4-hourly: last closed 4h window at minute 5
        self.schedule_4h = events.Rule(
            self,
            "DataExtraction4hSchedule",
            rule_name="bitcoin-market-4h",
            description="Incremental 4-hourly data writer",
            schedule=events.Schedule.cron(minute="5", hour="0/4"),
        )
        self.schedule_4h.add_target(
            targets.LambdaFunction(
                self.data_extraction_lambda,
                event=events.RuleTargetInput.from_object({"mode": "incremental", "interval": "4h"}),
                retry_attempts=2,
            )
        )

        # Daily: previous UTC day at 02:00 UTC
        self.schedule_1d = events.Rule(
            self,
            "DataExtraction1dSchedule",
            rule_name="bitcoin-market-1d",
            description="Incremental daily data writer",
            schedule=events.Schedule.cron(minute="0", hour="2"),
        )
        self.schedule_1d.add_target(
            targets.LambdaFunction(
                self.data_extraction_lambda,
                event=events.RuleTargetInput.from_object({"mode": "incremental", "interval": "1d"}),
                retry_attempts=2,
            )
        )

        # Weekly: previous full week on Monday 02:30 UTC
        self.schedule_1w = events.Rule(
            self,
            "DataExtraction1wSchedule",
            rule_name="bitcoin-market-1w",
            description="Incremental weekly data writer",
            schedule=events.Schedule.cron(minute="30", hour="2", week_day="MON"),
        )
        self.schedule_1w.add_target(
            targets.LambdaFunction(
                self.data_extraction_lambda,
                event=events.RuleTargetInput.from_object({"mode": "incremental", "interval": "1w"}),
                retry_attempts=2,
            )
        )

        # Manual trigger rule (console invoke substitute): run full generation
        self.manual_trigger_rule = events.Rule(
            self,
            "DataExtractionManualTrigger",
            rule_name="bitcoin-market-manual",
            description="Manual trigger for Bitcoin market data extraction (full)",
            schedule=events.Schedule.expression("rate(7 days)"),  # disabled by default if target removed
        )
        self.manual_trigger_rule.add_target(
            targets.LambdaFunction(
                self.data_extraction_lambda,
                event=events.RuleTargetInput.from_object({"mode": "full"}),
            )
        )

        # Outputs
        CfnOutput(
            self,
            "LambdaFunctionName",
            value=self.data_extraction_lambda.function_name,
            description="Bitcoin market data extractor Lambda function name",
        )

        CfnOutput(
            self,
            "DLQUrl",
            value=self.dlq.queue_url,
            description="Dead Letter Queue URL for failed invocations",
        )


