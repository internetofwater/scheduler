from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nhdplusv2huc12pphuc12pp0 import implnet_job_nhdplusv2huc12pphuc12pp0

@schedule(cron_schedule="0 16 8 * *", job=implnet_job_nhdplusv2huc12pphuc12pp0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nhdplusv2huc12pphuc12pp0(_context):
    run_config = {}
    return run_config
