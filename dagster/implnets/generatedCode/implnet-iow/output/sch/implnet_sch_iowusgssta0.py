from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_iowusgssta0 import implnet_job_iowusgssta0

@schedule(cron_schedule="0 18 2 * *", job=implnet_job_iowusgssta0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_iowusgssta0(_context):
    run_config = {}
    return run_config
