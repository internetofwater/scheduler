from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_exampleids0 import implnet_job_exampleids0

@schedule(cron_schedule="0 22 2 * *", job=implnet_job_exampleids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_exampleids0(_context):
    run_config = {}
    return run_config
