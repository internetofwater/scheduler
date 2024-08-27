from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihissnotelids0 import implnet_job_cuahsihissnotelids0

@schedule(cron_schedule="0 8 1 * *", job=implnet_job_cuahsihissnotelids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihissnotelids0(_context):
    run_config = {}
    return run_config
