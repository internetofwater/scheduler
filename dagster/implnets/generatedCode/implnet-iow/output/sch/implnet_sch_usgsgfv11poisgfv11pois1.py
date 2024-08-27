from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsgfv11poisgfv11pois1 import implnet_job_usgsgfv11poisgfv11pois1

@schedule(cron_schedule="0 6 13 * *", job=implnet_job_usgsgfv11poisgfv11pois1, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsgfv11poisgfv11pois1(_context):
    run_config = {}
    return run_config
