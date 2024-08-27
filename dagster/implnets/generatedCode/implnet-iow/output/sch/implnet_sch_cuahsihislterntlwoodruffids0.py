from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihislterntlwoodruffids0 import implnet_job_cuahsihislterntlwoodruffids0

@schedule(cron_schedule="0 20 16 * *", job=implnet_job_cuahsihislterntlwoodruffids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihislterntlwoodruffids0(_context):
    run_config = {}
    return run_config
