from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisunhsnowids0 import implnet_job_cuahsicuahsihisunhsnowids0

@schedule(cron_schedule="0 14 7 * *", job=implnet_job_cuahsicuahsihisunhsnowids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisunhsnowids0(_context):
    run_config = {}
    return run_config
