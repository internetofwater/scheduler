from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihissnotelids0 import implnet_job_cuahsicuahsihissnotelids0

@schedule(cron_schedule="0 8 1 * *", job=implnet_job_cuahsicuahsihissnotelids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihissnotelids0(_context):
    run_config = {}
    return run_config
