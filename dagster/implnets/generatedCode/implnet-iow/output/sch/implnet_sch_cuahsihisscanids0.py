from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisscanids0 import implnet_job_cuahsihisscanids0

@schedule(cron_schedule="0 20 15 * *", job=implnet_job_cuahsihisscanids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisscanids0(_context):
    run_config = {}
    return run_config
