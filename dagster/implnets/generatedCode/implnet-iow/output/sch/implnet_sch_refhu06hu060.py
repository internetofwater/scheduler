from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refhu06hu060 import implnet_job_refhu06hu060

@schedule(cron_schedule="0 20 4 * *", job=implnet_job_refhu06hu060, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refhu06hu060(_context):
    run_config = {}
    return run_config
