#!/usr/bin/env python3
import aws_cdk as cdk

from stacks.data_lake_stack import DataLakeStack
from stacks.ingestion_stack import IngestionStack
from stacks.observability_stack import ObservabilityStack

app = cdk.App()

# Environment configuration
env = cdk.Environment(
    account=app.node.try_get_context("account") or "055533307082",
    region=app.node.try_get_context("region") or "us-east-1",
)

# Data Lake Stack - Core infrastructure
data_lake = DataLakeStack(app, "BlockBoticsDataLake", env=env)

# Ingestion Stack - Lambda functions and data processing
ingestion = IngestionStack(app, "BlockBoticsIngestion", data_lake=data_lake, env=env)

# Observability Stack - Monitoring and dashboards
observability = ObservabilityStack(
    app, "BlockBoticsObservability", data_lake=data_lake, ingestion=ingestion, env=env
)

app.synth()
