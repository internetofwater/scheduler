from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisgleonauburnids0 import implnet_job_cuahsihisgleonauburnids0

@schedule(cron_schedule="0 16 19 * *", job=implnet_job_cuahsihisgleonauburnids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisgleonauburnids0(_context):
    run_config = {}
    return run_config
