from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisgonggaids0 import implnet_job_cuahsihisgonggaids0

@schedule(cron_schedule="0 2 1 * *", job=implnet_job_cuahsihisgonggaids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisgonggaids0(_context):
    run_config = {}
    return run_config
