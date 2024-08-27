from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihiscrwaids0 import implnet_job_cuahsihiscrwaids0

@schedule(cron_schedule="0 4 6 * *", job=implnet_job_cuahsihiscrwaids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihiscrwaids0(_context):
    run_config = {}
    return run_config
