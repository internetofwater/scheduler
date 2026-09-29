# Terraform does not support variables in the `backend` section
# you must use opentofu for this
pull_state:
	tofu init

# In order to apply changes, you must have the proper gcs credentials
# file and the tfvars file with all the values
# Additionally, sometimes the apply will not work if nothing has changed;
# to force a change, tweak one of the config values in the startup script
# for instance changing `GLEANER_CONCURRENT_SITEMAPS=` will force a change
apply:
	tofu apply