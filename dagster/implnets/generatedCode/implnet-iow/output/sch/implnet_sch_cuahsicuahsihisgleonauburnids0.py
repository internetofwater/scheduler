from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisgleonauburnids0 import implnet_job_cuahsicuahsihisgleonauburnids0

@schedule(cron_schedule="0 20 5 * *", job=implnet_job_cuahsicuahsihisgleonauburnids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisgleonauburnids0(_context):
    run_config = {}
    return run_config
