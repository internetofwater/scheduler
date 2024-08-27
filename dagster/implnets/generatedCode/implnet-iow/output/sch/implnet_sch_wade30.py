from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade30 import implnet_job_wade30

@schedule(cron_schedule="0 18 10 * *", job=implnet_job_wade30, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade30(_context):
    run_config = {}
    return run_config
