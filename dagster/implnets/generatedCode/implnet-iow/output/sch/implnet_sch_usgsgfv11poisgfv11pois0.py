from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsgfv11poisgfv11pois0 import implnet_job_usgsgfv11poisgfv11pois0

@schedule(cron_schedule="0 8 13 * *", job=implnet_job_usgsgfv11poisgfv11pois0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsgfv11poisgfv11pois0(_context):
    run_config = {}
    return run_config
