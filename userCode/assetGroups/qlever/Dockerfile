# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

FROM adfreiburg/qlever:commit-55c05d4

# Switch to root to install python
USER root

# Install Python
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Optional: switch back to the original user (often 'qlever')
USER qlever

# Copy your files
COPY index /home/qlever/index
COPY Qleverfile /home/qlever/Qleverfile

# Install qlever Python package
RUN pip3 install qlever --no-cache-dir --break-system-packages

# Set working directory to 'index'
WORKDIR /home/qlever/index

ENTRYPOINT ["bash", "-c", "ServerMain -i geoconnex -j 8 -p 8888 -m 5G -c 2G -e 1G -k 200 -s 30s"]
