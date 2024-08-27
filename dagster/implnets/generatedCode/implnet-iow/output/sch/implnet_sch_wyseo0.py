from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wyseo0 import implnet_job_wyseo0

@schedule(cron_schedule="0 16 2 * *", job=implnet_job_wyseo0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wyseo0(_context):
    run_config = {}
    return run_config
