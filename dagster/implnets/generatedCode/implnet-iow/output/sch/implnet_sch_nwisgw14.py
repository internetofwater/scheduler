from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw14 import implnet_job_nwisgw14

@schedule(cron_schedule="0 20 1 * *", job=implnet_job_nwisgw14, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw14(_context):
    run_config = {}
    return run_config
