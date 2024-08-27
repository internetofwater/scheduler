from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisieeratwilkesuniversityids0 import implnet_job_cuahsihisieeratwilkesuniversityids0

@schedule(cron_schedule="0 4 15 * *", job=implnet_job_cuahsihisieeratwilkesuniversityids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisieeratwilkesuniversityids0(_context):
    run_config = {}
    return run_config
