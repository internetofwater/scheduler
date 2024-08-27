from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_mainstems0 import implnet_job_mainstems0

@schedule(cron_schedule="0 16 24 * *", job=implnet_job_mainstems0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_mainstems0(_context):
    run_config = {}
    return run_config
