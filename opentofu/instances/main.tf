# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

resource "google_compute_instance" "harvest_vm" {
  name         = var.name
  machine_type = var.machine_type
  zone         = var.zone
  metadata = {
    enable-osconfig = "TRUE"
  }
  tags = ["https-server"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/${var.instance_os}"
      size  = var.disk_size
    }
  }

  network_interface {
    network = var.network_name
    access_config {
      nat_ip = var.static_ip
    }
  }

  service_account {
    email  = var.service_account_email
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    echo "Startup script initiated" > /var/log/startup.log

    # Step 1: Install Docker
    curl -sSL https://cgs-earth.github.io/script-cache/install_docker.sh | bash

    # Step 2: Install Scheduler
    echo "Downloading Scheduler" >> /var/log/startup.log
    apt install -y git
    sudo git clone --branch "${var.scheduler_version}" https://github.com/internetofwater/scheduler.git /opt/scheduler

    # Step 3: Set up Scheduler Environment
    cd /opt/scheduler
    cat <<ENV | sudo tee /opt/scheduler/.env > /dev/null

    # Gleaner
    HEADLESS_ENDPOINT=${var.headless_url}
    GLEANER_SITEMAP_INDEX=${var.sitemap_url}
    GLEANER_CONCURRENT_SITEMAPS=10
    GLEANER_SITEMAP_WORKERS=10
    GLEANER_LOG_LEVEL=ERROR
    GLEANER_SHACL_VALIDATOR_GRPC_ENDPOINT=scheduler_shacl_validator:50051
    GLEANER_USE_SHACL=true

    # Nabu
    NABU_PROFILING=false
    NABU_LOG_LEVEL=ERROR
    NABU_BATCH_SIZE=${var.nabu_batch_size}
    NABU_IMAGE=internetofwater/nabu:latest

    # Minio
    S3_ADDRESS=storage.googleapis.com
    S3_PORT=443
    S3_USE_SSL=true
    S3_SECRET_KEY=${var.s3_secret_key}
    S3_ACCESS_KEY=${var.s3_access_key}
    S3_DEFAULT_BUCKET=${var.s3_bucket}
    S3_REGION=${var.s3_region}

    # GraphDB
    TRIPLESTORE_URL=${var.graph_url}
    DATAGRAPH_REPOSITORY=${var.data_graph}
    PROVGRAPH_REPOSITORY=${var.prov_graph}

    # Database
    DAGSTER_POSTGRES_HOST=${var.database_host}
    DAGSTER_POSTGRES_USER=${var.database_user}
    DAGSTER_POSTGRES_PASSWORD=${var.database_password}
    DAGSTER_POSTGRES_DB=${var.database_name}

    # Integrations / Notifications
    DAGSTER_SLACK_TOKEN=${var.dagster_slack_token}
    LAKEFS_ENDPOINT_URL=${var.lakefs_endpoint}
    LAKEFS_ACCESS_KEY_ID=${var.lakefs_access_key}
    LAKEFS_SECRET_ACCESS_KEY=${var.lakefs_secret_key}
    ZENODO_ACCESS_TOKEN=${var.zenodo_access_token}
    ZENODO_SANDBOX_ACCESS_TOKEN=unset

    ENV

    echo "Pulling Scheduler images" >> /var/log/startup.log
    make pull

    # Step 4: Run Scheduler
    nohup make cloudProd &
    echo "Scheduler Started!" >> /var/log/startup.log

    if [ ${var.enable_public_url} != "false" ]; then
      # Step 5: Install Caddy
      curl -sSL https://cgs-earth.github.io/script-cache/install_caddy.sh | bash

      # Step 6: Run Caddy
      cat <<CADDYFILE | sudo tee /etc/caddy/Caddyfile > /dev/null
      ${var.url} {
              reverse_proxy :3000
      }
      CADDYFILE
      systemctl restart caddy
    fi

  EOF
}
