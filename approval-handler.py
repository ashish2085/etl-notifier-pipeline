import os
import json
import datetime
from google.oauth2 import service_account
from google.cloud import pubsub_v1
import functions_framework
from flask import abort, make_response, request

# Environment variables
PUBSUB_PROJECT = os.getenv("APPROVAL_PUBSUB_PROJECT", "fluence-devops")
PUBSUB_TOPIC = os.getenv("APPROVAL_PUBSUB_TOPIC", "pipeline-events")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "your-json-creds")

def load_credentials(creds_value):
    """
    Load credentials either from a JSON string or from a file path.
    """
    creds_value = creds_value.strip()
    if creds_value.startswith("{"):
        # Load from JSON string.
        credentials_info = json.loads(creds_value)
        return service_account.Credentials.from_service_account_info(credentials_info)
    else:
        # Load from file path.
        return service_account.Credentials.from_service_account_file(creds_value)

@functions_framework.http
def approval_handler(request):
    """
    HTTP endpoint to handle approval/rejection clicks and provide enough details for further processing.
    Only one request per event_id will be processed.
    """
    request_args = request.args

    # Required parameters.
    event_id = request_args.get("event_id")
    action = request_args.get("action")

    if not event_id or not action:
        return abort(400, "Missing required query parameters: event_id and action.")

    # Optional parameters.
    file_name = request_args.get("file_name")
    table_name = request_args.get("table_name")
    operation = request_args.get("operation")
    bucket = request_args.get("bucket")
    file_version = request_args.get("file_version")
    provided_timestamp = request_args.get("timestamp")
    
    # Build the payload with additional details.
    payload = {
        "event_id": event_id,
        "action": action.lower(),
        "file_name": file_name,
        "table_name": table_name,
        "operation": operation,
        "bucket": bucket,
        "file_version": file_version,
        "provided_timestamp": provided_timestamp,
        "approval_timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "remote_address": request.remote_addr
    }
    message_json = json.dumps(payload).encode("utf-8")

    try:
        # Load credentials using our helper function.
        credentials = None
        if GOOGLE_APPLICATION_CREDENTIALS:
            credentials = load_credentials(GOOGLE_APPLICATION_CREDENTIALS)

        # Initialize the Pub/Sub publisher client with explicit credentials.
        publisher = pubsub_v1.PublisherClient(credentials=credentials)
        topic_path = publisher.topic_path(PUBSUB_PROJECT, PUBSUB_TOPIC)
        future = publisher.publish(topic_path, message_json)
        future.result()  # Wait for confirmation.
    except Exception as ex:
        return make_response(f"Failed to publish message to Pub/Sub: {str(ex)}", 500)

    return make_response("Your approval has been recorded. Thank you.", 200)


# Requirement.txt
# functions-framework==3.*
# cloud-sql-python-connector
# psycopg2-binary
# pandas
# google-cloud-storage
# google-auth
# pg8000
# msal
# google-cloud-pubsub