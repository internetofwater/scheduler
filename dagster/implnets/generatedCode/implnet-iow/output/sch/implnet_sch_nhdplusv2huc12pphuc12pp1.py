from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nhdplusv2huc12pphuc12pp1 import implnet_job_nhdplusv2huc12pphuc12pp1

@schedule(cron_schedule="0 14 8 * *", job=implnet_job_nhdplusv2huc12pphuc12pp1, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nhdplusv2huc12pphuc12pp1(_context):
    run_config = {}
    return run_config
