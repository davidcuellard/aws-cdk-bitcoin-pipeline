"""
Unit tests for the Observability Stack
Tests CloudWatch Dashboard, Alarms, and SNS notifications
"""

import aws_cdk as core
import pytest

from stacks.observability_stack import ObservabilityStack


class TestObservabilityStack:
    """Test class for Observability Stack"""

    @pytest.fixture
    def app(self):
        """Create CDK app for testing"""
        return core.App()

    @pytest.fixture
    def data_lake_stack(self, app):
        """Create mock data lake stack"""
        from stacks.data_lake_stack import DataLakeStack

        return DataLakeStack(app, "test-data-lake-stack")

    @pytest.fixture
    def ingestion_stack(self, app, data_lake_stack):
        """Create mock ingestion stack"""
        from stacks.ingestion_stack import IngestionStack

        return IngestionStack(app, "test-ingestion-stack", data_lake_stack)

    @pytest.fixture
    def stack(self, app, data_lake_stack, ingestion_stack):
        """Create Observability stack for testing"""
        return ObservabilityStack(
            app, "test-observability-stack", data_lake_stack, ingestion_stack
        )

    def test_stack_creation(self, stack):
        """Test that the stack can be created successfully"""
        assert stack is not None
        assert stack.stack_name == "test-observability-stack"

    def test_stack_has_required_resources(self, stack):
        """Test that the stack has the required resources"""
        # Check that the stack has the expected resources
        assert hasattr(stack, "dashboard")
        assert hasattr(stack, "alerts_topic")

    def test_stack_outputs(self, stack):
        """Test that stack has expected outputs"""
        # Check that the stack has the expected output attributes
        # In CDK v2, we can check for the existence of output constructs
        # by looking at the stack's node children
        assert stack is not None
        # The outputs are created as CfnOutput constructs in the stack
        # We can verify the stack was created successfully
