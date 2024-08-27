from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_iowstategagesmtdnrc0 import implnet_job_iowstategagesmtdnrc0

@schedule(cron_schedule="0 8 2 * *", job=implnet_job_iowstategagesmtdnrc0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_iowstategagesmtdnrc0(_context):
    run_config = {}
    return run_config
