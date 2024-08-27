from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refhu04hu040 import implnet_job_refhu04hu040

@schedule(cron_schedule="0 10 3 * *", job=implnet_job_refhu04hu040, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refhu04hu040(_context):
    run_config = {}
    return run_config
