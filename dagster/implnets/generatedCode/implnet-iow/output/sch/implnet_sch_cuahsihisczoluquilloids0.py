from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisczoluquilloids0 import implnet_job_cuahsihisczoluquilloids0

@schedule(cron_schedule="0 18 5 * *", job=implnet_job_cuahsihisczoluquilloids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisczoluquilloids0(_context):
    run_config = {}
    return run_config
