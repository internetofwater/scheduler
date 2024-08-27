from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisglobalriversobservatoryids0 import implnet_job_cuahsicuahsihisglobalriversobservatoryids0

@schedule(cron_schedule="0 0 6 * *", job=implnet_job_cuahsicuahsihisglobalriversobservatoryids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisglobalriversobservatoryids0(_context):
    run_config = {}
    return run_config
