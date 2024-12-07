# File: main.py

from datetime import time
import yaml
import requests
import sys

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

def load_credentials():
    try:
        with open('credentials.yaml', 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print("Error: credentials.yaml file not found")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing credentials.yaml: {e}")
        sys.exit(1)

def validate_credentials(creds):
    required_harness = ['api_key', 'account_id', 'org_id', 'project_id', 'connector_name', 'repo_url']
    required_snowflake = ['account', 'user', 'password', 'warehouse', 'database']
    
    missing = []
    
    for field in required_harness:
        if field not in creds['harness']:
            missing.append(f'harness.{field}')
            
    for field in required_snowflake:
        if field not in creds['snowflake']:
            missing.append(f'snowflake.{field}')
            
    if missing:
        print("Error: Missing required credentials:")
        print("\n".join(f"- {field}" for field in missing))
        sys.exit(1)

def main():
    print("SnowPilot: Setup and trigger your own Harness pipeline for Snowflake migrations")
    print("Please ensure you have entered your credentials into credentials.yaml...")
    time.sleep(2)
    creds = load_credentials()
    validate_credentials(creds)
    args = type('Args', (), {
        'api_key': creds['harness']['api_key'],
        'acc_id': creds['harness']['account_id'],
        'org_id': creds['harness']['org_id'],
        'project_id': creds['harness']['project_id'],
        'connector_name': creds['harness']['connector_name'],
        'repo_url': creds['harness']['repo_url'],
        'snowflake_account': creds['snowflake']['account'],
        'snowflake_user': creds['snowflake']['user'],
        'snowflake_password': creds['snowflake']['password'],
        'snowflake_warehouse': creds['snowflake']['warehouse'],
        'snowflake_database': creds['snowflake']['database'],
        'snowflake_schema': creds['snowflake'].get('schema', 'PUBLIC')
    })()
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