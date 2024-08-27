from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade45 import implnet_job_wadewade45

@schedule(cron_schedule="0 12 12 * *", job=implnet_job_wadewade45, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade45(_context):
    run_config = {}
    return run_config
