import os
import json
import re
import datetime
import msal
import requests
from google.cloud.sql.connector import Connector, IPTypes
from google.oauth2 import service_account
import functions_framework
from contextlib import contextmanager

# Environment Variables
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "your-gcs-bucket-name")
CLOUD_SQL_INSTANCE = os.getenv("CLOUD_SQL_INSTANCE", "your-cloudsql-instance-connection-name")
DB_NAME = os.getenv("DB_NAME", "your-database-name")
DB_USER = os.getenv("DB_USER", "your-db-username")
DB_PASSWORD = os.getenv("DB_PASSWORD", "your-db-password")
DB_PORT = os.getenv("DB_PORT", "5432")
CUSTOM_SCHEMA = os.getenv("CUSTOM_SCHEMA", "your_custom_schema")
CONTROL_TABLE = f"{CUSTOM_SCHEMA}.processed_files"
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "your-json-credentials")

# Email settings (for sending approval emails)
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "sender_email")
PRODUCT_OWNER_EMAIL = os.getenv("PRODUCT_OWNER_EMAIL", "product_email")
APPROVAL_URL = os.getenv("APPROVAL_URL", "https://<your-region>-<your-project>.cloudfunctions.net/approval_gateway")

# ----------------------------------------------------------------------
# Helper Function: Database Connection Context Manager
# ----------------------------------------------------------------------
@contextmanager
def get_db_connection():
    """Context manager to get a DB connection and ensure cleanup."""
    connector = None
    conn = None
    try:
        credentials_info = json.loads(GOOGLE_APPLICATION_CREDENTIALS)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        connector = Connector(credentials=credentials)
        conn = connector.connect(
            CLOUD_SQL_INSTANCE,
            "pg8000",
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            ip_type=IPTypes.PRIVATE
        )
        print("Database connection established successfully.")
        yield conn
    except Exception as e:
        print(f"Error creating database connection: {e}")
        raise
    finally:
        if conn:
            conn.close()
        if connector:
            connector.close()

# ----------------------------------------------------------------------
# Functions that Reuse the Existing DB Connection
# ----------------------------------------------------------------------
def extract_table_name(file_name: str) -> str:
    """Derives a sanitized table name from the file name using the custom schema."""
    base_name = re.sub(r"\.[^.]*$", "", file_name)  # Remove extension
    sanitized_name = re.sub(r"[^a-zA-Z0-9_]", "_", base_name).lower()
    table_name = f"{CUSTOM_SCHEMA}.{sanitized_name}"
    print(f"Derived table name: {table_name} from file: {file_name}")
    return table_name

def get_next_file_version(conn, file_name: str) -> int:
    """Gets the next version number for the given file from the control table using the provided connection."""
    cursor = conn.cursor()
    try:
        query = f"SELECT MAX(file_version) FROM {CONTROL_TABLE} WHERE file_name = %s"
        cursor.execute(query, (file_name,))
        result = cursor.fetchone()
        max_version = result[0] if result and result[0] is not None else 0
        next_version = max_version + 1
        print(f"Next version for {file_name}: {next_version}")
        return next_version
    except Exception as e:
        print(f"Error fetching file version for {file_name}: {e}")
        raise
    finally:
        cursor.close()

def mark_file_as_processing(conn, file_name: str, event_id: str, bucket: str, operation: str) -> int:
    """
    Writes a pending record into the control table and returns the file version using the provided connection.
    """
    version = get_next_file_version(conn, file_name)
    cursor = conn.cursor()
    try:
        query = f"""
            INSERT INTO {CONTROL_TABLE} 
            (file_name, event_id, file_version, is_processed, bucket, operation, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (file_name, event_id, version, False, bucket, operation, 'pending'))
        conn.commit()
        print(f"Marked file '{file_name}' as pending (Event ID: {event_id}, Operation: {operation}, Version: {version}).")
        return version
    except Exception as e:
        print(f"Error marking file '{file_name}' as processing: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

# ----------------------------------------------------------------------
# Email and Access Token Functions
# ----------------------------------------------------------------------
def send_email(sender_email: str, recipient_email: str, subject: str, body_html: str) -> None:
    """Sends an email via Microsoft Graph API."""
    try:
        access_token = get_access_token()
        send_mail_url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
        email_data = {
            "message": {
                "subject": subject,
                "body": {"contentType": "HTML", "content": body_html},
                "toRecipients": [{"emailAddress": {"address": recipient_email}}]
            },
            "saveToSentItems": "true"
        }
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        response = requests.post(send_mail_url, headers=headers, json=email_data, timeout=30)
        response.raise_for_status()
        print(f"Email sent to {recipient_email} with subject '{subject}'.")
    except Exception as e:
        print(f"Error sending email: {e}")

def get_access_token() -> str:
    """Acquires an access token for Microsoft Graph API using MSAL (with a simple cache)."""
    import time
    now = time.time()
    if get_access_token._token and get_access_token._expiry > now:
        return get_access_token._token

    app = msal.ConfidentialClientApplication(
        os.getenv("CLIENT_ID", "your_client_id"),
        authority=f"https://login.microsoftonline.com/{os.getenv('TENANT_ID','your_tenant_id')}",
        client_credential=os.getenv("CLIENT_SECRET", "your_client_secret")
    )
    token_response = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" in token_response:
        get_access_token._token = token_response["access_token"]
        get_access_token._expiry = now + token_response.get("expires_in", 3600) - 300
        return get_access_token._token
    else:
        raise Exception("Unable to acquire Graph API access token.")
get_access_token._token = None
get_access_token._expiry = 0

def notify_product_owner(operation: str, table_name: str, file_name: str, event_id: str, bucket: str, file_version: int, provided_timestamp: str) -> None:
    """
    Sends an approval request email with Approve/Reject links that include
    all the necessary details for further processing.
    """
    subject = f"Approval Required: {operation.capitalize()} on {table_name}"
    # Construct query string parameters once to avoid repetition
    params = (
        f"event_id={event_id}&"
        f"file_name={operation}/{file_name}&"
        f"table_name={table_name}&"
        f"operation={operation}&"
        f"bucket={bucket}&"
        f"file_version={file_version}&"
        f"timestamp={provided_timestamp}"
    )
    approval_link = f"{APPROVAL_URL}?{params}&action=approve"
    rejection_link = f"{APPROVAL_URL}?{params}&action=reject"
    body_html = f"""
    <html>
    <body>
      <p>Dear Product Owner,</p>
      <p>An operation <strong>{operation}</strong> has been requested on table <strong>{table_name}</strong> via file <strong>{file_name}</strong> (Event ID: {event_id}).</p>
      <p>Details:</p>
      <ul>
        <li>Bucket: {bucket}</li>
        <li>File Version: {file_version}</li>
        <li>Timestamp: {provided_timestamp}</li>
      </ul>
      <p>Please click one of the following actions:</p>
      <p><a href="{approval_link}">Approve</a>&nbsp;&nbsp;&nbsp;<a href="{rejection_link}">Reject</a></p>
      <p>Best regards,<br>Your Data Engineering Team</p>
    </body>
    </html>
    """
    send_email(SENDER_EMAIL, PRODUCT_OWNER_EMAIL, subject, body_html)

# ----------------------------------------------------------------------
# Main Cloud Event Trigger Function on File Arrival
# ----------------------------------------------------------------------
@functions_framework.cloud_event
def gcs_trigger_handler(cloud_event) -> None:
    """
    Triggered on file upload in GCS.
    Uses a single DB connection for file versioning and approval record insertion,
    then sends an approval email with complete details.
    """
    try:
        event_data = cloud_event.data
        bucket_name = event_data["bucket"]
        file_path = event_data["name"]
        event_id = cloud_event["id"]
        print(f"Received event {event_id} for file gs://{bucket_name}/{file_path}")

        # Skip if the event is for a folder.
        if not file_path or file_path.endswith("/"):
            print(f"Ignoring folder/empty path event: {file_path}")
            return

        # Determine operation from the first folder in file path (e.g., "insert", "update", "delete")
        path_parts = file_path.split("/")
        if len(path_parts) < 2:
            print(f"Invalid file path structure: {file_path}")
            return

        operation = path_parts[0].lower()
        if operation not in {"insert", "update", "delete"}:
            print(f"Unsupported operation: {operation}")
            return

        actual_file_name = path_parts[-1]
        table_name = extract_table_name(actual_file_name)

        # Generate a timestamp for the event (UTC)
        ingestion_timestamp = datetime.datetime.utcnow().isoformat() + "Z"

        # Use a single connection for both version checking and insertion.
        with get_db_connection() as conn:
            file_version = mark_file_as_processing(conn, actual_file_name, event_id, bucket_name, operation)

        # Send approval email with additional details.
        notify_product_owner(operation, table_name, actual_file_name, event_id, bucket_name, file_version, ingestion_timestamp)
        print(f"Approval email sent for event {event_id}.")
    except Exception as e:
        print(f"Critical error in file trigger function: {e}")
        raise

# Requirement.txt
# functions-framework==3.*
# cloud-sql-python-connector
# psycopg2-binary
# pandas
# google-cloud-storage
# google-auth
# pg8000
# msal
# Flask