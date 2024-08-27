from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade49 import implnet_job_wade49

@schedule(cron_schedule="0 0 1 * *", job=implnet_job_wade49, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade49(_context):
    run_config = {}
    return run_config
