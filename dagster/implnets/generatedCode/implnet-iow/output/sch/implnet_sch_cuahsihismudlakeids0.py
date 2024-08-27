from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihismudlakeids0 import implnet_job_cuahsihismudlakeids0

@schedule(cron_schedule="0 4 3 * *", job=implnet_job_cuahsihismudlakeids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihismudlakeids0(_context):
    run_config = {}
    return run_config
