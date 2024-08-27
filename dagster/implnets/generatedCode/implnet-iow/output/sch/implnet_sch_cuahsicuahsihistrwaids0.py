from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihistrwaids0 import implnet_job_cuahsicuahsihistrwaids0

@schedule(cron_schedule="0 16 7 * *", job=implnet_job_cuahsicuahsihistrwaids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihistrwaids0(_context):
    run_config = {}
    return run_config
