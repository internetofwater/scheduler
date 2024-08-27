from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisbrazilucbids0 import implnet_job_cuahsihisbrazilucbids0

@schedule(cron_schedule="0 22 7 * *", job=implnet_job_cuahsihisbrazilucbids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisbrazilucbids0(_context):
    run_config = {}
    return run_config
