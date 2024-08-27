from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisnevcanids0 import implnet_job_cuahsihisnevcanids0

@schedule(cron_schedule="0 18 7 * *", job=implnet_job_cuahsihisnevcanids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisnevcanids0(_context):
    run_config = {}
    return run_config
