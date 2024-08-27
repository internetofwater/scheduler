from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade35 import implnet_job_wadewade35

@schedule(cron_schedule="0 2 10 * *", job=implnet_job_wadewade35, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade35(_context):
    run_config = {}
    return run_config
