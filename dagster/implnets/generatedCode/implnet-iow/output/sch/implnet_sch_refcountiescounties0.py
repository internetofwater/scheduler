from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refcountiescounties0 import implnet_job_refcountiescounties0

@schedule(cron_schedule="0 16 3 * *", job=implnet_job_refcountiescounties0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refcountiescounties0(_context):
    run_config = {}
    return run_config
