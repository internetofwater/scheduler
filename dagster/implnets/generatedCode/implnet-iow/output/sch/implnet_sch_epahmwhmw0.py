from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_epahmwhmw0 import implnet_job_epahmwhmw0

@schedule(cron_schedule="0 22 12 * *", job=implnet_job_epahmwhmw0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_epahmwhmw0(_context):
    run_config = {}
    return run_config
