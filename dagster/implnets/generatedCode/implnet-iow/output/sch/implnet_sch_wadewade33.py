from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade33 import implnet_job_wadewade33

@schedule(cron_schedule="0 22 9 * *", job=implnet_job_wadewade33, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade33(_context):
    run_config = {}
    return run_config
