from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisskcmilltownids0 import implnet_job_cuahsihisskcmilltownids0

@schedule(cron_schedule="0 12 12 * *", job=implnet_job_cuahsihisskcmilltownids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisskcmilltownids0(_context):
    run_config = {}
    return run_config
