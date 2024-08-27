from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihistarlandwaterqualityids0 import implnet_job_cuahsicuahsihistarlandwaterqualityids0

@schedule(cron_schedule="0 14 1 * *", job=implnet_job_cuahsicuahsihistarlandwaterqualityids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihistarlandwaterqualityids0(_context):
    run_config = {}
    return run_config
