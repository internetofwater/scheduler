from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisscanids0 import implnet_job_cuahsicuahsihisscanids0

@schedule(cron_schedule="0 6 6 * *", job=implnet_job_cuahsicuahsihisscanids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisscanids0(_context):
    run_config = {}
    return run_config
