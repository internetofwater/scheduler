#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

set -e

# Detect platform
OS=$(uname -s)
ARCH=$(uname -m)

# Map OS to rclone's naming convention
case "$OS" in
    Linux) OS="linux" ;;
    Darwin) OS="osx" ;;
    FreeBSD) OS="freebsd" ;;
    *) echo "Unsupported OS: $OS"; exit 1 ;;
esac

# Map architecture to rclone's naming convention
case "$ARCH" in
    x86_64) ARCH="amd64" ;;
    armv7l) ARCH="arm" ;;
    aarch64) ARCH="arm64" ;;
    arm64) ARCH="arm64" ;;
    i386) ARCH="386" ;;
    *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

# Construct download URL
FILENAME="rclone-current-${OS}-${ARCH}.zip"
URL="https://downloads.rclone.org/${FILENAME}"

echo "Downloading rclone for ${OS}-${ARCH}..."
curl -O "$URL"

# Unzip and install
unzip "$FILENAME"
cd rclone-*-${OS}-${ARCH}
cp rclone /usr/bin/
chown root:root /usr/bin/rclone
chmod 755 /usr/bin/rclone

echo "Installation complete!"
