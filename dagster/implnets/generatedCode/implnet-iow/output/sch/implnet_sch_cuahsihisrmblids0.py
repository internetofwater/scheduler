from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisrmblids0 import implnet_job_cuahsihisrmblids0

@schedule(cron_schedule="0 16 11 * *", job=implnet_job_cuahsihisrmblids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisrmblids0(_context):
    run_config = {}
    return run_config
