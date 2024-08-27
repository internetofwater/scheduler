from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade14 import implnet_job_wadewade14

@schedule(cron_schedule="0 22 11 * *", job=implnet_job_wadewade14, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade14(_context):
    run_config = {}
    return run_config
