from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refplacesplaces0 import implnet_job_refplacesplaces0

@schedule(cron_schedule="0 14 3 * *", job=implnet_job_refplacesplaces0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refplacesplaces0(_context):
    run_config = {}
    return run_config
