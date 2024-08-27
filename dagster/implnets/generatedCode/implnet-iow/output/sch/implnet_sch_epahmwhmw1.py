from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_epahmwhmw1 import implnet_job_epahmwhmw1

@schedule(cron_schedule="0 0 13 * *", job=implnet_job_epahmwhmw1, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_epahmwhmw1(_context):
    run_config = {}
    return run_config
