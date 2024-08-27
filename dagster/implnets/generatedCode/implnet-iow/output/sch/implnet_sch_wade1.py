from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade1 import implnet_job_wade1

@schedule(cron_schedule="0 16 3 * *", job=implnet_job_wade1, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade1(_context):
    run_config = {}
    return run_config
