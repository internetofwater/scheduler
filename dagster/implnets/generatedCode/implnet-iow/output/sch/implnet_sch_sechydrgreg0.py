from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_sechydrgreg0 import implnet_job_sechydrgreg0

@schedule(cron_schedule="0 8 3 * *", job=implnet_job_sechydrgreg0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_sechydrgreg0(_context):
    run_config = {}
    return run_config
