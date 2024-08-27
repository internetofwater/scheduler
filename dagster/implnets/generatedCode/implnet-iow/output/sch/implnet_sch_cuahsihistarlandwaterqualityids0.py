from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihistarlandwaterqualityids0 import implnet_job_cuahsihistarlandwaterqualityids0

@schedule(cron_schedule="0 8 14 * *", job=implnet_job_cuahsihistarlandwaterqualityids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihistarlandwaterqualityids0(_context):
    run_config = {}
    return run_config
