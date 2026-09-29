# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

resource "google_compute_instance" "qlever_vm" {
  name         = "${var.name}-qlever-vm"
  machine_type = var.qlever_machine_type
  zone         = var.zone
  allow_stopping_for_update = true
  metadata     = {
    enable-osconfig = "TRUE"
  }
  tags = [ "http-server", "https-server" ]

  boot_disk {
    initialize_params {
      image = "debian-cloud/${var.instance_os}"
      size  = var.qlever_disk_size
      type  = "pd-ssd"
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
    set -eux

    # Step 1: Update and install prerequisites

    useradd -r -m -s /bin/false qlever || true
    apt update -y
    apt install -y git wget google-cloud-cli python3-full
    curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
    bash add-google-cloud-ops-agent-repo.sh --also-install
    curl -sSL https://cgs-earth.github.io/script-cache/install_caddy.sh | bash

    # Step 2: Format SSD disk and mount to /data

    DISK="/dev/disk/by-id/google-persistent-disk-0"
    if ! blkid $DISK; then
        mkfs.ext4 -F $DISK
    fi
    mkdir -p -m 777 /data
    UUID=$(blkid -s UUID -o value $DISK)
    grep -q "$UUID" /etc/fstab || echo "UUID=$UUID /data ext4 defaults,nofail 0 2" >> /etc/fstab
    mount -a

    # Step 3: Pull data from Cloud Storage

    gsutil -m cp -r \
      gs://${var.s3_bucket}/geoconnex_index/* \
      /data/ || true
    chown -R qlever:qlever /data

    # Step 4: Install Qlever
    curl -sSL https://cgs-earth.github.io/script-cache/install_qlever.sh | bash

    # Step 5: Configure Caddy to reverse proxy to Qlever

    cat <<CADDYFILE | sudo tee /etc/caddy/Caddyfile > /dev/null
    ${var.qlever_url} {
            reverse_proxy :8888
    }
    CADDYFILE
    systemctl restart caddy

    # Step 6: Keep index live
    #
    # curl -sSL https://cgs-earth.github.io/script-cache/install_gcsfuse.sh | bash
    # gcsfuse --only-dir geoconnex_index ${var.s3_bucket} /data

    # Step 7: Start Qlever

    cd /data
    qlever-server -i geoconnex -j 16 -p 8888 -s 300s

  EOF
}
