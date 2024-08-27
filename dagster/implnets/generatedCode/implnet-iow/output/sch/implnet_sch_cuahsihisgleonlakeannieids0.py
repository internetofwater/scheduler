from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisgleonlakeannieids0 import implnet_job_cuahsihisgleonlakeannieids0

@schedule(cron_schedule="0 2 6 * *", job=implnet_job_cuahsihisgleonlakeannieids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisgleonlakeannieids0(_context):
    run_config = {}
    return run_config
