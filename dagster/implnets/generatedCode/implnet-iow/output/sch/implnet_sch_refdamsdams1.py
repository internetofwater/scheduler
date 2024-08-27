from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refdamsdams1 import implnet_job_refdamsdams1

@schedule(cron_schedule="0 4 3 * *", job=implnet_job_refdamsdams1, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refdamsdams1(_context):
    run_config = {}
    return run_config
