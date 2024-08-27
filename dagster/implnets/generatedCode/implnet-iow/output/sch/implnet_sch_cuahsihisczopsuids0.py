from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisczopsuids0 import implnet_job_cuahsihisczopsuids0

@schedule(cron_schedule="0 8 19 * *", job=implnet_job_cuahsihisczopsuids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisczopsuids0(_context):
    run_config = {}
    return run_config
