from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihismmaatacamaids0 import implnet_job_cuahsihismmaatacamaids0

@schedule(cron_schedule="0 0 9 * *", job=implnet_job_cuahsihismmaatacamaids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihismmaatacamaids0(_context):
    run_config = {}
    return run_config
