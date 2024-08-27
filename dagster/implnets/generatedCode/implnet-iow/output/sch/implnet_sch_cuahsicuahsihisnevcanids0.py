from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisnevcanids0 import implnet_job_cuahsicuahsihisnevcanids0

@schedule(cron_schedule="0 18 7 * *", job=implnet_job_cuahsicuahsihisnevcanids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisnevcanids0(_context):
    run_config = {}
    return run_config
