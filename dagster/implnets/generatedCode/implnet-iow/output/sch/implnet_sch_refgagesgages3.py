from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refgagesgages3 import implnet_job_refgagesgages3

@schedule(cron_schedule="0 8 4 * *", job=implnet_job_refgagesgages3, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refgagesgages3(_context):
    run_config = {}
    return run_config
