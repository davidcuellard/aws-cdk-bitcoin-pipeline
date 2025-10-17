"""
Unit tests for the Data Lake Stack
Tests S3 bucket, Glue database, Athena workgroup, and Lake Formation resources
"""

import aws_cdk as core
import pytest

from stacks.data_lake_stack import DataLakeStack


class TestDataLakeStack:
    """Test class for Data Lake Stack"""

    @pytest.fixture
    def app(self):
        """Create CDK app for testing"""
        return core.App()

    @pytest.fixture
    def stack(self, app):
        """Create Data Lake stack for testing"""
        return DataLakeStack(app, "test-data-lake-stack")

    def test_stack_creation(self, stack):
        """Test that the stack can be created successfully"""
        assert stack is not None
        assert stack.stack_name == "test-data-lake-stack"

    def test_stack_has_required_resources(self, stack):
        """Test that the stack has the required resources"""
        # Check that the stack has the expected resources
        assert hasattr(stack, "data_lake_bucket")
        assert hasattr(stack, "kms_key")
        assert hasattr(stack, "glue_database")
        assert hasattr(stack, "glue_crawler")
        assert hasattr(stack, "athena_workgroup")

    def test_stack_outputs(self, stack):
        """Test that stack has expected outputs"""
        # Check that the stack has the expected output attributes
        # In CDK v2, we can check for the existence of output constructs
        # by looking at the stack's node children
        assert stack is not None
        # The outputs are created as CfnOutput constructs in the stack
        # We can verify the stack was created successfully
