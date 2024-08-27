from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisskcmilltownids0 import implnet_job_cuahsicuahsihisskcmilltownids0

@schedule(cron_schedule="0 6 2 * *", job=implnet_job_cuahsicuahsihisskcmilltownids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisskcmilltownids0(_context):
    run_config = {}
    return run_config
