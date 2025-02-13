# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os
import platform
import shutil
import subprocess
import zipfile

import requests
from userCode.lib.env import assert_all_vars


def ensure_local_bin_in_path():
    """Ensure ~/.local/bin is in the PATH."""
    local_bin = os.path.expanduser("~/.local/bin")
    if local_bin not in os.environ["PATH"].split(os.pathsep):
        os.environ["PATH"] += os.pathsep + local_bin
    return local_bin


def download_rclone_binary():
    """Download the rclone binary to a user-writable location in the PATH."""
    local_bin = ensure_local_bin_in_path()
    os.makedirs(local_bin, exist_ok=True)

    # Check if rclone is already installed in ~/.local/bin
    rclone_path = os.path.join(local_bin, "rclone")
    if os.path.isfile(rclone_path):
        print(f"Rclone is already installed at {rclone_path}.")
        return

    # Determine the platform
    system = platform.system().lower()
    arch = platform.machine().lower()

    # Map system and architecture to the appropriate Rclone download URL
    if system == "linux" and arch in ("x86_64", "amd64"):
        download_url = "https://downloads.rclone.org/rclone-current-linux-amd64.zip"
    elif system == "linux" and arch in ("arm64", "aarch64"):
        download_url = "https://downloads.rclone.org/rclone-current-linux-arm64.zip"
    elif system == "darwin" and arch in ("arm64", "aarch64"):
        download_url = "https://downloads.rclone.org/rclone-current-osx-arm64.zip"
    else:
        raise SystemError(
            "Unsupported system or architecture: {} on {}".format(arch, system)
        )

    # Download the file
    def download_file(url, dest):
        print(f"Downloading Rclone from {url}...")
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(dest, "wb") as f:
                shutil.copyfileobj(response.raw, f)
            print("Download complete.")
        else:
            raise RuntimeError(
                f"Failed to download file. HTTP Status Code: {response.status_code}"
            )

    zip_file = "rclone.zip"
    download_file(download_url, zip_file)

    # Extract the downloaded zip file
    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        print("Extracting Rclone...")
        zip_ref.extractall("rclone_extracted")

    # Change to the extracted directory
    extracted_dir = next(
        (
            d
            for d in os.listdir("rclone_extracted")
            if os.path.isdir(os.path.join("rclone_extracted", d))
        ),
        None,
    )
    if not extracted_dir:
        raise FileNotFoundError("Extracted Rclone directory not found.")

    extracted_path = os.path.join("rclone_extracted", extracted_dir)

    # Copy the Rclone binary to ~/.local/bin
    rclone_binary = os.path.join(extracted_path, "rclone")
    if not os.path.isfile(rclone_binary):
        raise FileNotFoundError("Rclone binary not found in extracted directory.")

    print(f"Installing Rclone to {local_bin}...")
    shutil.copy(rclone_binary, rclone_path)
    os.chmod(rclone_path, 0o755)  # Set executable permissions

    print("Verifying Rclone installation...")
    subprocess.run(["rclone", "version"], check=True)

    os.remove(zip_file)
    shutil.rmtree("rclone_extracted")
    print("Installation complete.")


download_rclone_binary()
assert_all_vars()
