from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nmwdiose1 import implnet_job_nmwdiose1

@schedule(cron_schedule="0 12 8 * *", job=implnet_job_nmwdiose1, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nmwdiose1(_context):
    run_config = {}
    return run_config
