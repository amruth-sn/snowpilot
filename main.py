# File: main.py

import argparse
import requests
import yaml
import os

def create_harness_secret(api_key, secret_name, secret_value, org_id, project_id):
    url = f"https://app.harness.io/gateway/api/v1/secret?orgIdentifier={org_id}&projectIdentifier={project_id}"
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "name": secret_name,
        "type": "SecretText",
        "value": secret_value
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

def create_pipeline(api_key, pipeline_yaml, org_id, project_id):
    url = f"https://app.harness.io/gateway/api/pipelines/v2?accountIdentifier={acc_id}&orgIdentifier={org_id}&projectIdentifier={project_id}"
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/yaml"
    }
    response = requests.post(url, data=pipeline_yaml, headers=headers)
    response.raise_for_status()
    return response.json()

def trigger_pipeline(api_key, pipeline_id, org_id, project_id):
    url = f"https://app.harness.io/gateway/api/v1/pipelines/{pipeline_id}/execute?orgIdentifier={org_id}&projectIdentifier={project_id}"
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json"
    }
    response = requests.post(url, headers=headers)
    response.raise_for_status()
    return response.json()

def main():
    parser = argparse.ArgumentParser(description="Setup and Trigger Harness Pipeline for Snowflake Migrations")
    parser.add_argument('--api-key', required=True, help='Harness API Key')
    parser.add_argument('--org-id', required=True, help='Harness Organization Identifier')
    parser.add_argument('--project-id', required=True, help='Harness Project Identifier')
    parser.add_argument('--repo-url', required=True, help='User Git Repository URL')
    parser.add_argument('--snowflake-account', required=True, help='Snowflake Account')
    parser.add_argument('--snowflake-user', required=True, help='Snowflake User')
    parser.add_argument('--snowflake-password', required=True, help='Snowflake Password')
    parser.add_argument('--snowflake-warehouse', required=True, help='Snowflake Warehouse')
    parser.add_argument('--snowflake-database', required=True, help='Snowflake Database')
    parser.add_argument('--snowflake-schema', default='PUBLIC', help='Snowflake Schema (optional)')
    args = parser.parse_args()

    # Step 1: Create Secrets in Harness
    secrets = {
        'SNOWFLAKE_ACCOUNT': args.snowflake_account,
        'SNOWFLAKE_USER': args.snowflake_user,
        'SNOWFLAKE_PASSWORD': args.snowflake_password,
        'SNOWFLAKE_WAREHOUSE': args.snowflake_warehouse,
        'SNOWFLAKE_DATABASE': args.snowflake_database,
        'SNOWFLAKE_SCHEMA': args.snowflake_schema,
        'REPO_URL': args.repo_url
    }

    for secret_name, secret_value in secrets.items():
        print(f"Creating secret: {secret_name}")
        create_harness_secret(
            api_key=args.api_key,
            secret_name=secret_name,
            secret_value=secret_value,
            org_id=args.org_id,
            project_id=args.project_id
        )

    # Step 2: Prepare and Create Pipeline
    with open('pipeline.yaml', 'r') as file:
        pipeline_yaml = file.read()

    # Optionally replace placeholders if needed
    pipeline_yaml = pipeline_yaml.replace('${REPO_URL}', args.repo_url)

    print("Creating pipeline in Harness...")
    pipeline = create_pipeline(
        api_key=args.api_key,
        pipeline_yaml=pipeline_yaml,
        org_id=args.org_id,
        project_id=args.project_id
    )
    pipeline_id = pipeline.get('identifier')
    print(f"Pipeline created with ID: {pipeline_id}")

    # Step 3: Trigger the Pipeline
    print("Triggering the pipeline...")
    execution = trigger_pipeline(
        api_key=args.api_key,
        pipeline_id=pipeline_id,
        org_id=args.org_id,
        project_id=args.project_id
    )
    print(f"Pipeline execution started: {execution}")

if __name__ == "__main__":
    main()