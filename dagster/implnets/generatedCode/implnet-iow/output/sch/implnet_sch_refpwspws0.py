from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refpwspws0 import implnet_job_refpwspws0

@schedule(cron_schedule="0 0 4 * *", job=implnet_job_refpwspws0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refpwspws0(_context):
    run_config = {}
    return run_config
