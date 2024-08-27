from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_links0 import implnet_job_links0

@schedule(cron_schedule="0 8 27 * *", job=implnet_job_links0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_links0(_context):
    run_config = {}
    return run_config
