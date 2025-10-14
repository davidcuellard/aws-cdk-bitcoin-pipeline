#!/usr/bin/env python3
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_glue as glue,
    aws_athena as athena,
    aws_iam as iam,
    aws_lakeformation as lakeformation,
    aws_kms as kms,
    Duration,
    RemovalPolicy,
    CfnOutput,
)
from constructs import Construct


class DataLakeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # KMS key for encryption
        self.kms_key = kms.Key(
            self,
            "DataLakeKmsKey",
            description="KMS key for BlockBotics data lake encryption",
            enable_key_rotation=True,
        )

        # S3 Data Lake Bucket
        self.data_lake_bucket = s3.Bucket(
            self,
            "DataLakeBucket",
            bucket_name=f"data-pipeline-datalake-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.kms_key,
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="TransitionToIA",
                    enabled=True,
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30),
                        ),
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90),
                        ),
                    ],
                )
            ],
        )

        # Athena WorkGroup
        self.athena_workgroup = athena.CfnWorkGroup(
            self,
            "DataPipelineAthenaWorkGroup",
            name="data-pipeline-analytics",
            description="Athena workgroup for data pipeline analysis",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{self.data_lake_bucket.bucket_name}/athena-results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_KMS", kms_key=self.kms_key.key_arn
                    ),
                ),
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
                bytes_scanned_cutoff_per_query=1000000000,  # 1GB limit
            ),
        )

        # Glue Database
        self.glue_database = glue.CfnDatabase(
            self,
            "DataPipelineGlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="data_pipeline_analytics",
                description="Data pipeline analytics database",
                parameters={"classification": "parquet", "typeOfData": "file"},
            ),
        )

        # Glue Crawler Role
        self.crawler_role = iam.Role(
            self,
            "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
            inline_policies={
                "S3Access": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:DeleteObject",
                                "s3:ListBucket",
                            ],
                            resources=[
                                self.data_lake_bucket.bucket_arn,
                                f"{self.data_lake_bucket.bucket_arn}/*",
                            ],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["kms:Decrypt", "kms:GenerateDataKey"],
                            resources=[self.kms_key.key_arn],
                        ),
                    ]
                )
            },
        )

        # Glue Crawler
        self.glue_crawler = glue.CfnCrawler(
            self,
            "DataPipelineGlueCrawler",
            name="data-pipeline-crawler",
            role=self.crawler_role.role_arn,
            database_name=self.glue_database.ref,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{self.data_lake_bucket.bucket_name}/silver/",
                        exclusions=["**/.*"],
                    )
                ]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="DEPRECATE_IN_DATABASE",
            ),
            configuration='{"Version": 1.0, "CrawlerOutput": {"Partitions": {"AddOrUpdateBehavior": "InheritFromTable"}}}',
            lake_formation_configuration=glue.CfnCrawler.LakeFormationConfigurationProperty(
                use_lake_formation_credentials=False,
            ),
        )

        # Lake Formation Data Lake Administrator
        self.lake_formation_admin = lakeformation.CfnDataLakeSettings(
            self,
            "LakeFormationAdmin",
            admins=[
                lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=f"arn:aws:iam::{self.account}:root"
                )
            ],
        )

        # Lake Formation Data Location
        self.lake_formation_location = lakeformation.CfnResource(
            self,
            "LakeFormationDataLocation",
            resource_arn=self.data_lake_bucket.bucket_arn,
            use_service_linked_role=True,
        )

        # Lake Formation Database Permissions (already exist from previous deployment)

        # Lake Formation S3 Location Permissions (removed due to access issues)

        # Lake Formation Table Permissions for Glue Crawler
        # Note: Table permissions will be granted after tables are created by the crawler

        # Lake Formation Table Permissions (will be created after Glue Crawler runs)
        # Note: These permissions will be applied to tables created by the crawler

        # Outputs
        CfnOutput(
            self,
            "DataLakeBucketName",
            value=self.data_lake_bucket.bucket_name,
            description="S3 bucket for data lake storage",
        )

        CfnOutput(
            self,
            "GlueDatabaseName",
            value=self.glue_database.ref,
            description="Glue database name",
        )

        CfnOutput(
            self,
            "AthenaWorkGroupName",
            value=self.athena_workgroup.name,
            description="Athena workgroup name",
        )
