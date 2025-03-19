## Setup

- You will require a key generated from GCP's service account with the following permissions, `Storage Admin` & `Bigquery Admin`
- Navigate to the service account and click 'Manage keys'. Click on 'Add key' and generate and JSON file.
- Copy the contents of the key under './keys/my-creds.json'

## How to use the flows folder

- Spin up the containers in docker-compose.yml
- Navigate to Kestra's UI [localhost:8080]
- Click on flows on the left navigation bar
- Top-right, click `Create` button
- Copy contents, say `02_postgres_taxi.yml` and paste it in Kestra's Flows UI and click `Save` button on top right.
- Top-right, click `Execute` to run the flow.

Note: In KestraUI, navigate to namespace -> zoomcamp -> KV Store. Add a new key-value pair and name it `GCP_CREDS`. Paste the contents of './keys-creds.json' and hit save.