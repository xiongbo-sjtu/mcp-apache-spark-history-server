import unittest
from unittest.mock import MagicMock, patch

from tools import get_client_or_default, get_stage, get_slowest_jobs
from spark_client import SparkRestClient
from datetime import datetime, timedelta
from spark_types import JobData, StageData, TaskMetricDistributions


class TestTools(unittest.TestCase):
    def setUp(self):
        # Create mock context
        self.mock_ctx = MagicMock()
        self.mock_lifespan_context = MagicMock()
        self.mock_ctx.request_context.lifespan_context = self.mock_lifespan_context

        # Create mock clients
        self.mock_client1 = MagicMock(spec=SparkRestClient)
        self.mock_client2 = MagicMock(spec=SparkRestClient)

        # Set up clients dictionary
        self.mock_lifespan_context.clients = {
            "server1": self.mock_client1,
            "server2": self.mock_client2,
        }

    def test_get_client_with_name(self):
        """Test getting a client by name"""
        self.mock_lifespan_context.default_client = self.mock_client1

        # Get client by name
        client = get_client_or_default(self.mock_ctx, "server2")

        # Should return the requested client
        self.assertEqual(client, self.mock_client2)

    def test_get_default_client(self):
        """Test getting the default client when no name is provided"""
        self.mock_lifespan_context.default_client = self.mock_client1

        # Get client without specifying name
        client = get_client_or_default(self.mock_ctx)

        # Should return the default client
        self.assertEqual(client, self.mock_client1)

    def test_get_client_not_found_with_default(self):
        """Test behavior when requested client is not found but default exists"""
        self.mock_lifespan_context.default_client = self.mock_client1

        # Get non-existent client
        client = get_client_or_default(self.mock_ctx, "non_existent_server")

        # Should fall back to default client
        self.assertEqual(client, self.mock_client1)

    def test_no_client_found(self):
        """Test error when no client is found and no default exists"""
        self.mock_lifespan_context.default_client = None

        # Try to get non-existent client with no default
        with self.assertRaises(ValueError) as context:
            get_client_or_default(self.mock_ctx, "non_existent_server")

        self.assertIn("No Spark client found", str(context.exception))

    def test_no_default_client(self):
        """Test error when no name is provided and no default exists"""
        self.mock_lifespan_context.default_client = None

        # Try to get default client when none exists
        with self.assertRaises(ValueError) as context:
            get_client_or_default(self.mock_ctx)

        self.assertIn("No Spark client found", str(context.exception))

    @patch("tools.get_client_or_default")
    def test_get_slowest_jobs_empty(self, mock_get_client):
        """Test get_slowest_jobs when no jobs are found"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.get_jobs.return_value = []
        mock_get_client.return_value = mock_client

        # Call the function

        result = get_slowest_jobs("app-123", n=3)

        # Verify results
        self.assertEqual(result, [])
        mock_client.get_jobs.assert_called_once_with(app_id="app-123")

    @patch("tools.get_client_or_default")
    def test_get_slowest_jobs_exclude_running(self, mock_get_client):
        """Test get_slowest_jobs excluding running jobs"""
        # Setup mock client and jobs
        mock_client = MagicMock()

        # Create mock jobs with different durations and statuses
        job1 = MagicMock(spec=JobData)
        job1.status = "RUNNING"
        job1.submission_time = datetime.now() - timedelta(minutes=10)
        job1.completion_time = None

        job2 = MagicMock(spec=JobData)
        job2.status = "SUCCEEDED"
        job2.submission_time = datetime.now() - timedelta(minutes=5)
        job2.completion_time = datetime.now() - timedelta(minutes=3)  # 2 min duration

        job3 = MagicMock(spec=JobData)
        job3.status = "SUCCEEDED"
        job3.submission_time = datetime.now() - timedelta(minutes=10)
        job3.completion_time = datetime.now() - timedelta(minutes=5)  # 5 min duration

        job4 = MagicMock(spec=JobData)
        job4.status = "FAILED"
        job4.submission_time = datetime.now() - timedelta(minutes=8)
        job4.completion_time = datetime.now() - timedelta(minutes=7)  # 1 min duration

        mock_client.get_jobs.return_value = [job1, job2, job3, job4]
        mock_get_client.return_value = mock_client

        # Call the function with include_running=False (default)

        result = get_slowest_jobs("app-123", n=2)

        # Verify results - should return job3 and job2 (in that order)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], job3)  # Longest duration (5 min)
        self.assertEqual(result[1], job2)  # Second longest (2 min)

        # Running job (job1) should be excluded
        self.assertNotIn(job1, result)

    @patch("tools.get_client_or_default")
    def test_get_slowest_jobs_include_running(self, mock_get_client):
        """Test get_slowest_jobs including running jobs"""
        # Setup mock client and jobs
        mock_client = MagicMock()

        # Create mock jobs with different durations and statuses
        job1 = MagicMock(spec=JobData)
        job1.status = "RUNNING"
        job1.submission_time = datetime.now() - timedelta(
            minutes=20
        )  # Running for 20 min
        job1.completion_time = None

        job2 = MagicMock(spec=JobData)
        job2.status = "SUCCEEDED"
        job2.submission_time = datetime.now() - timedelta(minutes=5)
        job2.completion_time = datetime.now() - timedelta(minutes=3)  # 2 min duration

        job3 = MagicMock(spec=JobData)
        job3.status = "SUCCEEDED"
        job3.submission_time = datetime.now() - timedelta(minutes=10)
        job3.completion_time = datetime.now() - timedelta(minutes=5)  # 5 min duration

        mock_client.get_jobs.return_value = [job1, job2, job3]
        mock_get_client.return_value = mock_client

        # Call the function with include_running=True

        result = get_slowest_jobs("app-123", include_running=True, n=2)

        # Verify results - should include the running job
        self.assertEqual(len(result), 2)
        # Running job should be included but will have duration 0 since completion_time is None
        # So job3 and job2 should be returned
        self.assertEqual(result[0], job3)
        self.assertEqual(result[1], job2)

    @patch("tools.get_client_or_default")
    def test_get_slowest_jobs_limit_results(self, mock_get_client):
        """Test get_slowest_jobs limits results to n"""
        # Setup mock client and jobs
        mock_client = MagicMock()

        # Create 5 mock jobs with different durations
        jobs = []
        for i in range(5):
            job = MagicMock(spec=JobData)
            job.status = "SUCCEEDED"
            job.submission_time = datetime.now() - timedelta(minutes=10)
            # Different completion times to create different durations
            job.completion_time = datetime.now() - timedelta(minutes=10 - i)
            jobs.append(job)

        mock_client.get_jobs.return_value = jobs
        mock_get_client.return_value = mock_client

        # Call the function with n=3

        result = get_slowest_jobs("app-123", n=3)

        # Verify results - should return only 3 jobs
        self.assertEqual(len(result), 3)

    @patch("tools.get_client_or_default")
    def test_get_stage_with_attempt_id(self, mock_get_client):
        """Test get_stage with a specific attempt ID"""
        # Setup mock client
        mock_client = MagicMock()
        mock_stage = MagicMock(spec=StageData)
        mock_stage.task_metrics_distributions = None
        # Explicitly set the attempt_id attribute on the mock
        mock_stage.attempt_id = 0
        mock_client.get_stage_attempt.return_value = mock_stage
        mock_get_client.return_value = mock_client

        # Call the function with attempt_id
        from tools import get_stage

        result = get_stage("app-123", stage_id=1, attempt_id=0)

        # Verify results
        self.assertEqual(result, mock_stage)
        mock_client.get_stage_attempt.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            attempt_id=0,
            details=False,
            with_summaries=False,
        )

    @patch("tools.get_client_or_default")
    def test_get_stage_without_attempt_id_single_stage(self, mock_get_client):
        """Test get_stage without attempt ID when a single stage is returned"""
        # Setup mock client
        mock_client = MagicMock()
        mock_stage = MagicMock(spec=StageData)
        mock_stage.task_metrics_distributions = None
        # Explicitly set the attempt_id attribute on the mock
        mock_stage.attempt_id = 0
        mock_client.get_stage.return_value = mock_stage
        mock_get_client.return_value = mock_client

        # Call the function without attempt_id
        from tools import get_stage

        result = get_stage("app-123", stage_id=1)

        # Verify results
        self.assertEqual(result, mock_stage)
        mock_client.get_stage.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            details=False,
            with_summaries=False,
        )

    @patch("tools.get_client_or_default")
    def test_get_stage_without_attempt_id_multiple_stages(self, mock_get_client):
        """Test get_stage without attempt ID when multiple stages are returned"""
        # Setup mock client
        mock_client = MagicMock()

        # Create mock stages with different attempt IDs
        mock_stage1 = MagicMock(spec=StageData)
        mock_stage1.attempt_id = 0
        mock_stage1.task_metrics_distributions = None

        mock_stage2 = MagicMock(spec=StageData)
        mock_stage2.attempt_id = 1
        mock_stage2.task_metrics_distributions = None

        mock_client.get_stage.return_value = [mock_stage1, mock_stage2]
        mock_get_client.return_value = mock_client

        # Call the function without attempt_id
        from tools import get_stage

        result = get_stage("app-123", stage_id=1)

        # Verify results - should return the stage with highest attempt_id
        self.assertEqual(result, mock_stage2)
        mock_client.get_stage.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            details=False,
            with_summaries=False,
        )

    @patch("tools.get_client_or_default")
    def test_get_stage_with_summaries_missing_metrics(self, mock_get_client):
        """Test get_stage with summaries when metrics distributions are missing"""
        # Setup mock client
        mock_client = MagicMock()
        mock_stage = MagicMock(spec=StageData)
        # Explicitly set the attempt_id attribute on the mock
        mock_stage.attempt_id = 0
        # Set task_metrics_distributions to None to trigger the fetch
        mock_stage.task_metrics_distributions = None

        mock_summary = MagicMock(spec=TaskMetricDistributions)

        mock_client.get_stage_attempt.return_value = mock_stage
        mock_client.get_stage_task_summary.return_value = mock_summary
        mock_get_client.return_value = mock_client

        # Call the function with with_summaries=True
        from tools import get_stage

        result = get_stage("app-123", stage_id=1, attempt_id=0, with_summaries=True)

        # Verify results
        self.assertEqual(result, mock_stage)
        self.assertEqual(result.task_metrics_distributions, mock_summary)

        mock_client.get_stage_attempt.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            attempt_id=0,
            details=False,
            with_summaries=True,
        )

        mock_client.get_stage_task_summary.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            attempt_id=0,
        )

    @patch("tools.get_client_or_default")
    def test_get_stage_no_stages_found(self, mock_get_client):
        """Test get_stage when no stages are found"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.get_stage.return_value = []
        mock_get_client.return_value = mock_client

        with self.assertRaises(ValueError) as context:
            get_stage("app-123", stage_id=1)

        self.assertIn("No stage found with ID 1", str(context.exception))
