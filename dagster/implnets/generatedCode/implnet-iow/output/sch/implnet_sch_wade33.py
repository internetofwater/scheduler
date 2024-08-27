from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade33 import implnet_job_wade33

@schedule(cron_schedule="0 22 9 * *", job=implnet_job_wade33, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade33(_context):
    run_config = {}
    return run_config
