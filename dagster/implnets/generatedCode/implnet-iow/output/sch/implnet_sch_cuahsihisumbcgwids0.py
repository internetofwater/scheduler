from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisumbcgwids0 import implnet_job_cuahsihisumbcgwids0

@schedule(cron_schedule="0 16 12 * *", job=implnet_job_cuahsihisumbcgwids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisumbcgwids0(_context):
    run_config = {}
    return run_config
