from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_huc12pp1 import implnet_job_huc12pp1

@schedule(cron_schedule="0 12 25 * *", job=implnet_job_huc12pp1, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_huc12pp1(_context):
    run_config = {}
    return run_config
