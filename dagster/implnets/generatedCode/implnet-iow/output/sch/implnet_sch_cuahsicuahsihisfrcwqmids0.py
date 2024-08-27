from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisfrcwqmids0 import implnet_job_cuahsicuahsihisfrcwqmids0

@schedule(cron_schedule="0 20 6 * *", job=implnet_job_cuahsicuahsihisfrcwqmids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisfrcwqmids0(_context):
    run_config = {}
    return run_config
