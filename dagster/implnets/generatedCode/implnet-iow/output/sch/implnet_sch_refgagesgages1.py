from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refgagesgages1 import implnet_job_refgagesgages1

@schedule(cron_schedule="0 10 4 * *", job=implnet_job_refgagesgages1, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refgagesgages1(_context):
    run_config = {}
    return run_config
