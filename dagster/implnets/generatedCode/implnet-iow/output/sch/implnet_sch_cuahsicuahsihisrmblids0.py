from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisrmblids0 import implnet_job_cuahsicuahsihisrmblids0

@schedule(cron_schedule="0 20 4 * *", job=implnet_job_cuahsicuahsihisrmblids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisrmblids0(_context):
    run_config = {}
    return run_config
