from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisczoluquilloids0 import implnet_job_cuahsicuahsihisczoluquilloids0

@schedule(cron_schedule="0 18 5 * *", job=implnet_job_cuahsicuahsihisczoluquilloids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisczoluquilloids0(_context):
    run_config = {}
    return run_config
