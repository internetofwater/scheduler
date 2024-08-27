from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihismobilecrowdhydrologyids0 import implnet_job_cuahsihismobilecrowdhydrologyids0

@schedule(cron_schedule="0 8 5 * *", job=implnet_job_cuahsihismobilecrowdhydrologyids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihismobilecrowdhydrologyids0(_context):
    run_config = {}
    return run_config
