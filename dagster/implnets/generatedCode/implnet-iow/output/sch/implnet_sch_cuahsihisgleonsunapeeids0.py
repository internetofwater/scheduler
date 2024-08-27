from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisgleonsunapeeids0 import implnet_job_cuahsihisgleonsunapeeids0

@schedule(cron_schedule="0 10 1 * *", job=implnet_job_cuahsihisgleonsunapeeids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisgleonsunapeeids0(_context):
    run_config = {}
    return run_config
