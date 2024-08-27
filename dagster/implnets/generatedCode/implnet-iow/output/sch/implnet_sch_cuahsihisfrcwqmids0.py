from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisfrcwqmids0 import implnet_job_cuahsihisfrcwqmids0

@schedule(cron_schedule="0 8 17 * *", job=implnet_job_cuahsihisfrcwqmids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisfrcwqmids0(_context):
    run_config = {}
    return run_config
