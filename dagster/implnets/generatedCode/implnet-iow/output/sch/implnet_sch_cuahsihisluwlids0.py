from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisluwlids0 import implnet_job_cuahsihisluwlids0

@schedule(cron_schedule="0 12 7 * *", job=implnet_job_cuahsihisluwlids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisluwlids0(_context):
    run_config = {}
    return run_config
