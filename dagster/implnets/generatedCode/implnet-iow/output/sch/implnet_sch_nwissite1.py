from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwissite1 import implnet_job_nwissite1

@schedule(cron_schedule="0 14 13 * *", job=implnet_job_nwissite1, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwissite1(_context):
    run_config = {}
    return run_config
