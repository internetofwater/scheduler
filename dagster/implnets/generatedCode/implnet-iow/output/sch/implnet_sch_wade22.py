from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade22 import implnet_job_wade22

@schedule(cron_schedule="0 12 10 * *", job=implnet_job_wade22, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade22(_context):
    run_config = {}
    return run_config
