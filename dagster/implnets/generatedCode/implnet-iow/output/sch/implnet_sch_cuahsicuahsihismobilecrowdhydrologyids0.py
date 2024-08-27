from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihismobilecrowdhydrologyids0 import implnet_job_cuahsicuahsihismobilecrowdhydrologyids0

@schedule(cron_schedule="0 8 5 * *", job=implnet_job_cuahsicuahsihismobilecrowdhydrologyids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihismobilecrowdhydrologyids0(_context):
    run_config = {}
    return run_config
