from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade14 import implnet_job_wade14

@schedule(cron_schedule="0 8 2 * *", job=implnet_job_wade14, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade14(_context):
    run_config = {}
    return run_config
