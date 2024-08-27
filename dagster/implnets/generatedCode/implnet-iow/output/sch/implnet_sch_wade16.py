from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade16 import implnet_job_wade16

@schedule(cron_schedule="0 8 9 * *", job=implnet_job_wade16, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade16(_context):
    run_config = {}
    return run_config
