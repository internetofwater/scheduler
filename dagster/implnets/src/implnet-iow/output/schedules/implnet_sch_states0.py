from dagster import schedule

from jobs.implnet_jobs_states0 import implnet_job_states0

@schedule(cron_schedule="0 6 * * 3", job=implnet_job_states0, execution_timezone="US/Central")
def implnet_sch_states0(_context):
    run_config = {}
    return run_config