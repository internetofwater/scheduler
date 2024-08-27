from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihislterntlwoodruffids0 import implnet_job_cuahsicuahsihislterntlwoodruffids0

@schedule(cron_schedule="0 8 4 * *", job=implnet_job_cuahsicuahsihislterntlwoodruffids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihislterntlwoodruffids0(_context):
    run_config = {}
    return run_config
