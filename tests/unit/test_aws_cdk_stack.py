"""
Unit tests for the main AWS CDK Stack
Tests the complete application stack integration
"""

import aws_cdk as core
import aws_cdk.assertions as assertions
import pytest

from app import BlockBoticsApp


class TestMainApp:
    """Test class for the main application"""

    @pytest.fixture
    def app(self):
        """Create CDK app for testing"""
        return core.App()

    @pytest.fixture
    def stack(self, app):
        """Create main app stack for testing"""
        return BlockBoticsApp(app, "test-blockbotics-app")

    @pytest.fixture
    def template(self, stack):
        """Create CDK template for assertions"""
        return assertions.Template.from_stack(stack)

    def test_app_has_all_stacks(self, stack):
        """Test that app has all required stacks"""
        # Check that all stacks are created
        assert hasattr(stack, 'data_lake_stack')
        assert hasattr(stack, 'ingestion_stack')
        assert hasattr(stack, 'observability_stack')

    def test_data_lake_stack_created(self, template):
        """Test that data lake stack resources are created"""
        # Check for S3 bucket
        template.has_resource_properties("AWS::S3::Bucket", {
            "BucketEncryption": assertions.Match.any_value()
        })
        
        # Check for Glue database
        template.has_resource_properties("AWS::Glue::Database", {
            "DatabaseInput": {
                "Name": "data_pipeline_analytics"
            }
        })

    def test_ingestion_stack_created(self, template):
        """Test that ingestion stack resources are created"""
        # Check for Lambda function
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "bitcoin-market-extractor"
        })

    def test_observability_stack_created(self, template):
        """Test that observability stack resources are created"""
        # Check for CloudWatch dashboard
        template.has_resource_properties("AWS::CloudWatch::Dashboard", {
            "DashboardName": "BlockBotics-DataPipeline"
        })

    def test_all_resources_have_tags(self, template):
        """Test that all resources have proper tags"""
        # This is a basic test - in practice, you'd check specific resources
        template.has_resource_properties("AWS::S3::Bucket", {
            "Tags": assertions.Match.any_value()
        })

    def test_stack_outputs(self, stack):
        """Test that stack has expected outputs"""
        outputs = stack.template.outputs
        # Check for key outputs
        assert len(outputs) > 0
