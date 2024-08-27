from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade29 import implnet_job_wade29

@schedule(cron_schedule="0 18 11 * *", job=implnet_job_wade29, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade29(_context):
    run_config = {}
    return run_config
