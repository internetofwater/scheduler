from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisunhsnowids0 import implnet_job_cuahsihisunhsnowids0

@schedule(cron_schedule="0 14 7 * *", job=implnet_job_cuahsihisunhsnowids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisunhsnowids0(_context):
    run_config = {}
    return run_config
