from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade43 import implnet_job_wade43

@schedule(cron_schedule="0 10 9 * *", job=implnet_job_wade43, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade43(_context):
    run_config = {}
    return run_config
