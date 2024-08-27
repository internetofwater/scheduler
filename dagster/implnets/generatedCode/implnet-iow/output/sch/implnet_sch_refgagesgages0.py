from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refgagesgages0 import implnet_job_refgagesgages0

@schedule(cron_schedule="0 14 4 * *", job=implnet_job_refgagesgages0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refgagesgages0(_context):
    run_config = {}
    return run_config
