from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade21 import implnet_job_wade21

@schedule(cron_schedule="0 0 10 * *", job=implnet_job_wade21, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade21(_context):
    run_config = {}
    return run_config
