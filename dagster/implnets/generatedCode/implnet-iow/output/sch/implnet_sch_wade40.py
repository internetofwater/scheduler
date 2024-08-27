from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade40 import implnet_job_wade40

@schedule(cron_schedule="0 4 10 * *", job=implnet_job_wade40, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade40(_context):
    run_config = {}
    return run_config
