from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisluwlids0 import implnet_job_cuahsicuahsihisluwlids0

@schedule(cron_schedule="0 12 7 * *", job=implnet_job_cuahsicuahsihisluwlids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisluwlids0(_context):
    run_config = {}
    return run_config
