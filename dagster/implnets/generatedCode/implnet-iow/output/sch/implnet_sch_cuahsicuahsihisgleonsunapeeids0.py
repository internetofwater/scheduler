from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisgleonsunapeeids0 import implnet_job_cuahsicuahsihisgleonsunapeeids0

@schedule(cron_schedule="0 10 1 * *", job=implnet_job_cuahsicuahsihisgleonsunapeeids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisgleonsunapeeids0(_context):
    run_config = {}
    return run_config
