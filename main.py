# File: main.py

import argparse
import requests
import os
import json

def create_harness_secret(api_key, secret_name, secret_value, org_id, project_id, acc_id):
    url = f"https://app.harness.io/gateway/ng/api/v2/secrets/text?accountIdentifier={acc_id}"
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "secret": {
            "name": secret_name,
            "identifier": secret_name.replace('_', '').lower(),
            "orgIdentifier": org_id,
            "projectIdentifier": project_id,
            "tags": {},
            "type": "SecretText",
            "spec": {
                "secretManagerIdentifier": "harnessSecretManager",
                "valueType": "Inline",
                "value": secret_value
            }
        }
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

def create_git_connector(api_key, connector_name, repo_url, org_id, project_id, acc_id):
    url = f'https://app.harness.io/gateway/ng/api/connectors?accountIdentifier={acc_id}'
    headers = {
        'x-api-key': api_key,
        'Content-Type': 'application/json'
    }
    payload = {
        "connector": {
            "name": connector_name,
            "identifier": connector_name.replace('_', '').lower(),
            "description": "",
            "orgIdentifier": org_id,
            "projectIdentifier": project_id,
            "type": "Github",
            "spec": {
                "url": repo_url,
                "type": "Http",
                "authentication": {
                    "type": "Anonymous"
                },
                "apiAccess": None,
                "delegateSelectors": []
            }
        }
    }
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()

def create_pipeline(api_key, pipeline_yaml, org_id, project_id, acc_id):
    url = f"https://app.harness.io/gateway/pipeline/api/pipelines/v2?accountIdentifier={acc_id}&orgIdentifier={org_id}&projectIdentifier={project_id}"
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/yaml"
    }
    response = requests.post(url, data=pipeline_yaml, headers=headers)
    response.raise_for_status()
    return response.json()

def create_git_trigger(api_key, pipeline_id, org_id, project_id, acc_id, repo_connector_id):
    url = f"https://app.harness.io/gateway/pipeline/api/triggers/v2?accountIdentifier={acc_id}&orgIdentifier={org_id}&projectIdentifier={project_id}"
    
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/yaml"
    }
    
    # Payload for Git push trigger
    trigger_yaml = f"""
trigger:
  name: Git Push Trigger
  identifier: git_push_trigger_{pipeline_id}
  enabled: true
  description: Trigger pipeline on Git push
  orgIdentifier: {org_id}
  projectIdentifier: {project_id}
  pipelineIdentifier: {pipeline_id}
  source:
    type: Webhook
    spec:
      type: Github
      spec:
        type: Push
        connectorRef: {repo_connector_id}
        parameters:
          branch: main
    inputYaml: |
      pipeline:
        identifier: {pipeline_id}
        properties:
          ci:
            codebase:
              build:
                type: branch
                spec:
                  branch: main
"""
    
    response = requests.post(url, data=trigger_yaml, headers=headers)
    response.raise_for_status()
    return response.json()

def main():
    parser = argparse.ArgumentParser(description="Setup and Trigger Harness Pipeline for Snowflake Migrations")
    parser.add_argument('--api-key', required=True, help='Harness API Key')
    parser.add_argument('--acc-id', required=True, help='Harness Account Identifier')
    parser.add_argument('--org-id', required=True, help='Harness Organization Identifier')
    parser.add_argument('--project-id', required=True, help='Harness Project Identifier')
    parser.add_argument('--connector-name', required=True, help='Name for the Git Connector')
    parser.add_argument('--repo-url', required=True, help='User Git Repository URL')
    parser.add_argument('--snowflake-account', required=True, help='Snowflake Account')
    parser.add_argument('--snowflake-user', required=True, help='Snowflake User')
    parser.add_argument('--snowflake-password', required=True, help='Snowflake Password')
    parser.add_argument('--snowflake-warehouse', required=True, help='Snowflake Warehouse')
    parser.add_argument('--snowflake-database', required=True, help='Snowflake Database')
    parser.add_argument('--snowflake-schema', default='PUBLIC', help='Snowflake Schema (optional)')
    args = parser.parse_args()

    secrets = {
        'SNOWFLAKE_ACCOUNT': args.snowflake_account,
        'SNOWFLAKE_USER': args.snowflake_user,
        'SNOWFLAKE_PASSWORD': args.snowflake_password,
        'SNOWFLAKE_WAREHOUSE': args.snowflake_warehouse,
        'SNOWFLAKE_DATABASE': args.snowflake_database,
        'SNOWFLAKE_SCHEMA': args.snowflake_schema
    }

    # Step 1: Create secrets in Harness
    for secret_name, secret_value in secrets.items():
        print(f"Creating secret: {secret_name}")
        create_harness_secret(
            api_key=args.api_key,
            secret_name=secret_name,
            secret_value=secret_value,
            org_id=args.org_id,
            project_id=args.project_id,
            acc_id=args.acc_id
        )

    # Step 2: Create Git connector
    print(f"Creating Git connector: {args.connector_name}")
    create_git_connector(
        api_key=args.api_key,
        connector_name=args.connector_name,
        repo_url=args.repo_url,
        org_id=args.org_id,
        project_id=args.project_id,
        acc_id=args.acc_id
    )

    # Step 3: Read pipeline YAML and replace placeholders
    with open('pipeline.yaml', 'r') as file:
        pipeline_yaml = file.read()

    pipeline_yaml = pipeline_yaml.replace('${REPO_CONNECTOR}', args.connector_name.replace('_', '').lower())

    # Step 4: Create pipeline in Harness
    print("Creating pipeline in Harness...")
    pipeline_response = create_pipeline(
        api_key=args.api_key,
        pipeline_yaml=pipeline_yaml,
        org_id=args.org_id,
        project_id=args.project_id,
        acc_id=args.acc_id
    )
    pipeline_identifier = pipeline_response['data']['identifier']
    print(f"Pipeline created with ID: {pipeline_identifier}")

    # Step 5: Trigger the pipeline
    print("Creating Git push trigger...")
    create_git_trigger(
        api_key=args.api_key,
        pipeline_id=pipeline_identifier,
        org_id=args.org_id, 
        project_id=args.project_id,
        acc_id=args.acc_id,
        repo_connector_id=args.connector_name.replace('_', '').lower()
    )
    
    print("Git push trigger created successfully")

    # Step 6: Generate Delegate YAML for user to deploy
    print("Generating Delegate YAML...")
    delegate_yaml_url = f"https://app.harness.io/gateway/ng/api/delegates/yaml?accountIdentifier={args.acc_id}"
    headers = {
        "x-api-key": args.api_key,
        "Content-Type": "application/json"
    }
    delegate_name = input("Enter a name for your Delegate: ")
    payload = {
        "name": delegate_name,
        "identifier": delegate_name.replace('_', '').lower(),
        "description": "Delegate created via script",
        "orgIdentifier": args.org_id,
        "projectIdentifier": args.project_id,
        "delegateType": "Kubernetes",
        "helmChartYaml": False
    }
    response = requests.post(delegate_yaml_url, headers=headers, json=payload)
    response.raise_for_status()

    delegate_yaml = response.json()['data']
    with open('harness-delegate.yaml', 'w') as f:
        f.write(delegate_yaml)
    print(f"Delegate YAML generated: harness-delegate.yaml")
    print("Please apply this YAML file to your Kubernetes cluster to deploy the Harness Delegate.")

if __name__ == "__main__":
    main()