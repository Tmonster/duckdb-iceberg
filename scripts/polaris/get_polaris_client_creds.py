import re
import os

log_content = ""
# Read the log file (hopefully it isn't too big)
with open("polaris_catalog/user_credentials.json", "r") as file:
    log_content = file.read()

# Regular expression to capture the credentials
match = re.search(r"{\"clientId\": \"(\w+)\", \"clientSecret\": \"(\w+)\"}", log_content)

if match:
    clientId = match.group(1)
    clientSecret = match.group(2)
    if clientId and clientSecret:
        # Write client_id and client_secret to separate files
        with open("polaris_client_id.txt", "w") as id_file:
            print(f"clientId {clientId}")
            id_file.write(clientId)

        with open("polaris_client_secret.txt", "w") as secret_file:
            print(f"clientSecret {clientSecret}")
            secret_file.write(clientSecret)

else:
    print("Credentials not found in the log file.")
    exit(1)
