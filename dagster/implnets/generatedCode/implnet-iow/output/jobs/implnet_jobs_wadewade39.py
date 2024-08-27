from dagster import job

from ops.implnet_ops_wadewade39 import harvest_wadewade39

@job
def implnet_job_wadewade39():
    harvest_wadewade39()
