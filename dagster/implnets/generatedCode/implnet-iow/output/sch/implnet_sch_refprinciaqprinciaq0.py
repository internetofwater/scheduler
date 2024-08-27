from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refprinciaqprinciaq0 import implnet_job_refprinciaqprinciaq0

@schedule(cron_schedule="0 12 3 * *", job=implnet_job_refprinciaqprinciaq0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refprinciaqprinciaq0(_context):
    run_config = {}
    return run_config
