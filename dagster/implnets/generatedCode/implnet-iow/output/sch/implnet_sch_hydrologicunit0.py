from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_hydrologicunit0 import implnet_job_hydrologicunit0

@schedule(cron_schedule="0 20 6 * *", job=implnet_job_hydrologicunit0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_hydrologicunit0(_context):
    run_config = {}
    return run_config
