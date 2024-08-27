from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisumbcwqids0 import implnet_job_cuahsihisumbcwqids0

@schedule(cron_schedule="0 22 5 * *", job=implnet_job_cuahsihisumbcwqids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisumbcwqids0(_context):
    run_config = {}
    return run_config
