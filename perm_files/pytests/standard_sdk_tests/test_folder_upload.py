import os, sys
import shutil


# 1. Setup a dummy project folder
project_name = "my_titan_project"

from titan_sdk.titan_sdk import TitanClient

if not os.path.exists(project_path):
    os.makedirs(project_path)
    # Create a few dummy files
    with open(os.path.join(project_path, "main.py"), "w") as f:
        f.write("print('Main running')")
    with open(os.path.join(project_path, "helper.py"), "w") as f:
        f.write("print('Helper loaded')")
    print(f"📁 Created test folder at: {project_path}")

# 2. Upload it using the SDK
client = TitanClient()
print(f"🚀 Uploading Project Folder: {project_name}...")

# This should Zip -> Upload -> Return Success
resp = client.upload_project_folder(project_path)
print(f"Server Response: {resp}")

# 3. Cleanup (Optional)
# shutil.rmtree(project_path)