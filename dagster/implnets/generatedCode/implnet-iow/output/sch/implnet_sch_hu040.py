from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_hu040 import implnet_job_hu040

@schedule(cron_schedule="0 12 22 * *", job=implnet_job_hu040, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_hu040(_context):
    run_config = {}
    return run_config
