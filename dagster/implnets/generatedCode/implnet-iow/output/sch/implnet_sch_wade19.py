from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade19 import implnet_job_wade19

@schedule(cron_schedule="0 0 3 * *", job=implnet_job_wade19, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade19(_context):
    run_config = {}
    return run_config
