from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihismopexids0 import implnet_job_cuahsihismopexids0

@schedule(cron_schedule="0 8 2 * *", job=implnet_job_cuahsihismopexids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihismopexids0(_context):
    run_config = {}
    return run_config
