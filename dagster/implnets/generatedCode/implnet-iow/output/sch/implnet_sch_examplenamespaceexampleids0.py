from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_examplenamespaceexampleids0 import implnet_job_examplenamespaceexampleids0

@schedule(cron_schedule="0 22 2 * *", job=implnet_job_examplenamespaceexampleids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_examplenamespaceexampleids0(_context):
    run_config = {}
    return run_config
