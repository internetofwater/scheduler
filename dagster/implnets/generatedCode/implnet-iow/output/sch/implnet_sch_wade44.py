from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade44 import implnet_job_wade44

@schedule(cron_schedule="0 12 9 * *", job=implnet_job_wade44, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade44(_context):
    run_config = {}
    return run_config
