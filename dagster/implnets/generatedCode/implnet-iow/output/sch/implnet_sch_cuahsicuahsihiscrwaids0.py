from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihiscrwaids0 import implnet_job_cuahsicuahsihiscrwaids0

@schedule(cron_schedule="0 4 6 * *", job=implnet_job_cuahsicuahsihiscrwaids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihiscrwaids0(_context):
    run_config = {}
    return run_config
