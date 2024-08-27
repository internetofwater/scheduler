from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisgleonlakeannieids0 import implnet_job_cuahsicuahsihisgleonlakeannieids0

@schedule(cron_schedule="0 2 6 * *", job=implnet_job_cuahsicuahsihisgleonlakeannieids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisgleonlakeannieids0(_context):
    run_config = {}
    return run_config
