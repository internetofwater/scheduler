from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade28 import implnet_job_wade28

@schedule(cron_schedule="0 8 12 * *", job=implnet_job_wade28, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade28(_context):
    run_config = {}
    return run_config
