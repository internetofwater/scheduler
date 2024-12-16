import os
import shutil
import stat
import urllib.request
import zipfile

# Define the URL and file names
url = "https://downloads.rclone.org/rclone-current-linux-amd64.zip"
zip_file = "rclone-current-linux-amd64.zip"
extract_dir = "rclone-extracted"

# Download the file
print("Downloading rclone...")
urllib.request.urlretrieve(url, zip_file)

# Extract the ZIP file
print("Extracting rclone...")
with zipfile.ZipFile(zip_file, "r") as zip_ref:
    zip_ref.extractall(extract_dir)

# Change to the extracted directory
for entry in os.listdir(extract_dir):
    if entry.startswith("rclone") and os.path.isdir(os.path.join(extract_dir, entry)):
        extracted_path = os.path.join(extract_dir, entry)
        break

rclone_path = os.path.join(extracted_path, "rclone")

# Copy rclone binary to /usr/bin
destination = "/usr/bin/rclone"
print(f"Copying {rclone_path} to {destination}...")
shutil.copy2(rclone_path, destination)

# Set permissions
print("Setting permissions...")
os.chmod(
    destination,
    stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
)

# Clean up
print("Cleaning up...")
os.remove(zip_file)
shutil.rmtree(extract_dir)

# Test installation
print("Testing rclone installation...")
os.system("rclone version")
