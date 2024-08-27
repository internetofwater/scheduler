from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_chyldchyldpilotids0 import implnet_job_chyldchyldpilotids0

@schedule(cron_schedule="0 0 8 * *", job=implnet_job_chyldchyldpilotids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_chyldchyldpilotids0(_context):
    run_config = {}
    return run_config
