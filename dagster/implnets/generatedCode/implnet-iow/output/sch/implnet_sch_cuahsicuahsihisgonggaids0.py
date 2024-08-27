from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisgonggaids0 import implnet_job_cuahsicuahsihisgonggaids0

@schedule(cron_schedule="0 2 1 * *", job=implnet_job_cuahsicuahsihisgonggaids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisgonggaids0(_context):
    run_config = {}
    return run_config
