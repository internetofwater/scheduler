from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nmwdiosenmwdiose0 import implnet_job_nmwdiosenmwdiose0

@schedule(cron_schedule="0 4 8 * *", job=implnet_job_nmwdiosenmwdiose0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nmwdiosenmwdiose0(_context):
    run_config = {}
    return run_config
