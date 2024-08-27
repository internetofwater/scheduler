from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refgagesgages2 import implnet_job_refgagesgages2

@schedule(cron_schedule="0 12 4 * *", job=implnet_job_refgagesgages2, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refgagesgages2(_context):
    run_config = {}
    return run_config
