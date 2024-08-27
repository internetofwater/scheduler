from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisieeratwilkesuniversityids0 import implnet_job_cuahsicuahsihisieeratwilkesuniversityids0

@schedule(cron_schedule="0 12 2 * *", job=implnet_job_cuahsicuahsihisieeratwilkesuniversityids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisieeratwilkesuniversityids0(_context):
    run_config = {}
    return run_config
