from typing import Optional, List, Union

from app import mcp
from spark_types import (
    JobExecutionStatus,
    JobData,
    StageData,
    StageStatus,
    TaskMetricDistributions,
    ExecutionData,
    SQLExecutionStatus,
    ApplicationInfo,
)


def get_client_or_default(ctx, server_name: Optional[str] = None):
    """
    Get a client by server name or the default client if no name is provided.

    Args:
        ctx: The MCP context
        server_name: Optional server name

    Returns:
        SparkRestClient: The requested client or default client

    Raises:
        ValueError: If no client is found
    """
    clients = ctx.request_context.lifespan_context.clients
    default_client = ctx.request_context.lifespan_context.default_client

    if server_name:
        client = clients.get(server_name)
        if client:
            return client

    if default_client:
        return default_client

    raise ValueError(
        "No Spark client found. Please specify a valid server name or set a default server."
    )


@mcp.tool()
def get_application(spark_id: str, server: Optional[str] = None) -> ApplicationInfo:
    """
    Get detailed information about a specific Spark application.

    Retrieves comprehensive information about a Spark application including its
    status, resource usage, duration, and attempt details.

    Args:
        spark_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        ApplicationInfo object containing application details
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    return client.get_application(spark_id)


@mcp.tool()
def get_jobs(
    spark_id: str, server: Optional[str] = None, status: Optional[list[str]] = None
) -> list:
    """
    Get a list of all jobs for a Spark application.

    Args:
        spark_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        status: Optional list of job status values to filter by

    Returns:
        List of JobData objects for the application
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Convert string status values to JobExecutionStatus enum if provided
    job_statuses = None
    if status:
        job_statuses = [JobExecutionStatus.from_string(s) for s in status]

    return client.get_jobs(app_id=spark_id, status=job_statuses)


@mcp.tool()
def get_slowest_jobs(
    spark_id: str,
    server: Optional[str] = None,
    include_running: bool = False,
    n: int = 5,
) -> List[JobData]:
    """
    Get the N slowest jobs for a Spark application.

    Retrieves all jobs for the application and returns the ones with the longest duration.

    Args:
        spark_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        include_running: Whether to include running jobs in the search
        n: Number of slowest jobs to return (default: 5)

    Returns:
        List of JobData objects for the slowest jobs, or empty list if no jobs found
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Get all jobs
    jobs = client.get_jobs(app_id=spark_id)

    if not jobs:
        return []

    # Filter out running jobs if not included
    if not include_running:
        jobs = [job for job in jobs if job.status != JobExecutionStatus.RUNNING.value]

    if not jobs:
        return []

    # Sort jobs by duration (descending)
    def get_job_duration(job):
        if job.completion_time and job.submission_time:
            return (job.completion_time - job.submission_time).total_seconds()
        return 0

    sorted_jobs = sorted(jobs, key=get_job_duration, reverse=True)

    return sorted_jobs[:n]


@mcp.tool()
def get_stages(
    spark_id: str,
    server: Optional[str] = None,
    status: Optional[list[str]] = None,
    details: bool = False,
    with_summaries: bool = False,
) -> list:
    """
    Get a list of all stages for a Spark application.

    Retrieves information about stages in a Spark application with options to filter
    by status and include additional details and summary metrics.

    Args:
        spark_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        status: Optional list of stage status values to filter by
        details: Whether to include task details in the response
        with_summaries: Whether to include summary metrics in the response

    Returns:
        List of StageData objects for the application
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Convert string status values to StageStatus enum if provided
    stage_statuses = None
    if status:
        stage_statuses = [StageStatus.from_string(s) for s in status]

    return client.get_stages(
        app_id=spark_id,
        status=stage_statuses,
        details=details,
        with_summaries=with_summaries,
    )


@mcp.tool()
def get_slowest_stages(
    spark_id: str,
    server: Optional[str] = None,
    include_running: bool = False,
    n: int = 5,
) -> List[StageData]:
    """
    Get the N slowest stages for a Spark application.

    Retrieves all stages for the application and returns the ones with the longest duration.

    Args:
        spark_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        include_running: Whether to include running stages in the search
        n: Number of slowest stages to return (default: 5)

    Returns:
        List of StageData objects for the slowest stages, or empty list if no stages found
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Get all stages with details
    stages = client.get_stages(app_id=spark_id, details=True)

    if not stages:
        return []

    # Filter out running stages if not included
    if not include_running:
        stages = [stage for stage in stages if stage.status != "RUNNING"]

    if not stages:
        return []

    # Sort stages by duration (descending) and take top N
    # Calculate duration from completion_time and submission_time
    def get_stage_duration(stage):
        if stage.completion_time and stage.submission_time:
            return (stage.completion_time - stage.submission_time).total_seconds()
        return 0

    sorted_stages = sorted(stages, key=get_stage_duration, reverse=True)

    return sorted_stages[:n]


@mcp.tool()
def get_stage(
    spark_id: str,
    stage_id: int,
    attempt_id: Optional[int] = None,
    server: Optional[str] = None,
    details: bool = True,
    with_summaries: bool = False,
) -> Union[StageData, List[StageData]]:
    """
    Get information about a specific stage.

    Args:
        spark_id: The Spark application ID
        stage_id: The stage ID
        attempt_id: Optional stage attempt ID (if not provided, returns all attempts)
        server: Optional server name to use (uses default if not specified)
        details: Whether to include task details
        with_summaries: Whether to include summary metrics

    Returns:
        StageData object if attempt_id is provided, otherwise a list of StageData objects
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    if attempt_id is not None:
        return client.get_stage_attempt(
            app_id=spark_id,
            stage_id=stage_id,
            attempt_id=attempt_id,
            details=details,
            with_summaries=with_summaries,
        )
    else:
        return client.get_stage(
            app_id=spark_id,
            stage_id=stage_id,
            details=details,
            with_summaries=with_summaries,
        )


@mcp.tool()
def get_environment(spark_id: str, server: Optional[str] = None):
    """
    Get the comprehensive Spark runtime configuration for a Spark application.

    Details including JVM information, Spark properties, system properties,
    classpath entries, and environment variables.

    Args:
        spark_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        ApplicationEnvironmentInfo object containing environment details
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    return client.get_environment(app_id=spark_id)


@mcp.tool()
def get_executors(
    spark_id: str, server: Optional[str] = None, include_inactive: bool = False
):
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    if include_inactive:
        return client.get_all_executors(app_id=spark_id)
    else:
        return client.get_executors(app_id=spark_id)


@mcp.tool()
def get_executor(spark_id: str, executor_id: str, server: Optional[str] = None):
    """
    Get information about a specific executor.

    Retrieves detailed information about a single executor including resource allocation,
    task statistics, memory usage, and performance metrics.

    Args:
        spark_id: The Spark application ID
        executor_id: The executor ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        ExecutorSummary object containing executor details or None if not found
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Get all executors and find the one with matching ID
    executors = client.get_all_executors(app_id=spark_id)

    for executor in executors:
        if executor.id == executor_id:
            return executor

    return None


@mcp.tool()
def get_executor_summary(spark_id: str, server: Optional[str] = None):
    """
    Aggregates metrics across all executors for a Spark application.

    Retrieves all executors (active and inactive) and calculates summary statistics
    including memory usage, disk usage, task counts, and performance metrics.

    Args:
        spark_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing aggregated executor metrics
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    executors = client.get_all_executors(app_id=spark_id)

    summary = {
        "total_executors": len(executors),
        "active_executors": sum(1 for e in executors if e.is_active),
        "memory_used": 0,
        "disk_used": 0,
        "completed_tasks": 0,
        "failed_tasks": 0,
        "total_duration": 0,
        "total_gc_time": 0,
        "total_input_bytes": 0,
        "total_shuffle_read": 0,
        "total_shuffle_write": 0,
    }

    # Aggregate metrics from all executors
    for executor in executors:
        summary["memory_used"] += (
            executor.memory_metrics.used_on_heap_storage_memory
            + executor.memory_metrics.used_off_heap_storage_memory
        )
        summary["disk_used"] += executor.disk_used
        summary["completed_tasks"] += executor.completed_tasks
        summary["failed_tasks"] += executor.failed_tasks
        summary["total_duration"] += executor.total_duration
        summary["total_gc_time"] += executor.total_gc_time
        summary["total_input_bytes"] += executor.total_input_bytes
        summary["total_shuffle_read"] += executor.total_shuffle_read
        summary["total_shuffle_write"] += executor.total_shuffle_write

    return summary


@mcp.tool()
def get_stage_task_summary(
    spark_id: str,
    stage_id: int,
    attempt_id: int = 0,
    server: Optional[str] = None,
    quantiles: str = "0.05,0.25,0.5,0.75,0.95",
) -> TaskMetricDistributions:
    """
    Get a summary of task metrics for a specific stage.

    Retrieves statistical distributions of task metrics for a stage, including
    execution times, memory usage, I/O metrics, and shuffle metrics.

    Args:
        spark_id: The Spark application ID
        stage_id: The stage ID
        attempt_id: The stage attempt ID (default: 0)
        server: Optional server name to use (uses default if not specified)
        quantiles: Comma-separated list of quantiles to use for summary metrics

    Returns:
        TaskMetricDistributions object containing metric distributions
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    return client.get_stage_task_summary(
        app_id=spark_id, stage_id=stage_id, attempt_id=attempt_id, quantiles=quantiles
    )


@mcp.tool()
def get_slowest_sql_queries(
    spark_id: str,
    server: Optional[str] = None,
    attempt_id: Optional[str] = None,
    top_n: int = 1,
    page_size: int = 100,
    include_running: bool = False,
) -> List[ExecutionData]:
    """
    Get a summary of the top N slowest SQL queries for an application.

    Args:
        spark_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        attempt_id: Optional attempt ID
        top_n: Number of slowest queries to return
        page_size: Number of executions to fetch per page
        include_running: Whether to include running queries in the results

    Returns:
        List of ExecutionData objects for the slowest queries
        The total time metric (shown with time unit "m" for minutes) represents cumulative CPU time spent across all parallel tasks performing the scan operation
        This should be interpreted alongside the min/median/max metrics, which show the distribution of individual task durations.
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    all_executions: List[ExecutionData] = []
    offset = 0

    # Fetch all pages of SQL executions
    while True:
        executions: List[ExecutionData] = client.get_sql_list(
            app_id=spark_id,
            attempt_id=attempt_id,
            details=True,
            plan_description=False,
            offset=offset,
            length=page_size,
        )

        if not executions:
            break

        all_executions.extend(executions)
        offset += page_size

        # If we got fewer executions than the page size, we've reached the end
        if len(executions) < page_size:
            break

    # Filter out running queries if not included
    if not include_running:
        all_executions = [
            e for e in all_executions if e.status != SQLExecutionStatus.RUNNING.value
        ]

    # Sort by duration (descending) and take top N
    sorted_executions = sorted(all_executions, key=lambda e: e.duration, reverse=True)
    return sorted_executions[:2]
