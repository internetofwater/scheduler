from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade47 import implnet_job_wade47

@schedule(cron_schedule="0 8 10 * *", job=implnet_job_wade47, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade47(_context):
    run_config = {}
    return run_config
