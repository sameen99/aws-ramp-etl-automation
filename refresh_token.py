#!/usr/bin/python3
import requests
import base64
from dotenv import load_dotenv
import os

# Load existing environment variables from the .env file
env_file_path = r"/home/sameen/qb_scripts/.env_access_ramp"
load_dotenv(dotenv_path=env_file_path)

# Fetch credentials from the .env file
client_id = os.getenv('RAMP_CLIENT_ID')
client_secret = os.getenv('RAMP_CLIENT_SECRET')

# Check if credentials are loaded properly
if not client_id or not client_secret:
    print("Error: Client ID or Client Secret is missing in the .env file.")
    exit(1)

# Ramp API endpoint
endpoint = "https://api.ramp.com/developer/v1/token"

# Generate base64 encoded token
RAMP_API_TOKEN = base64.b64encode(str.encode(f"{client_id}:{client_secret}")).decode()

# Headers for the token request
headers = {
    "Accept": "application/json",
    "Authorization": f"Basic {RAMP_API_TOKEN}",
    "Content-Type": "application/x-www-form-urlencoded",
}

# Payload for the token request
payload = {
    "grant_type": "client_credentials",
    "scope": "business:read transactions:read bills:read",
}

# Make the POST request to get the access token
response = requests.post(endpoint, headers=headers, data=payload)

# Check if the request was successful
if response.status_code == 200:
    response_data = response.json()
    new_access_token = response_data["access_token"]
    
    # Check if file is empty
    if os.stat(env_file_path).st_size == 0:
        # If empty, add the access token
        with open(env_file_path, "w") as file:
            file.write(f"RAMP_API_TOKEN={new_access_token}\n")
    else:
        # Update the env.txt file with the new access token
        with open(env_file_path, "r") as file:
            env_lines = file.readlines()

        with open(env_file_path, "w") as file:
            token_updated = False
            for line in env_lines:
                if line.startswith("RAMP_API_TOKEN"):
                    file.write(f"RAMP_API_TOKEN={new_access_token}\n")
                    token_updated = True
                else:
                    file.write(line)
            
            # If token was not found, add it at the end
            if not token_updated:
                file.write(f"RAMP_API_TOKEN={new_access_token}\n")

    print("Access token refreshed and stored successfully!")
else:
    print(f"Failed to refresh token: {response.status_code}, {response.text}")
