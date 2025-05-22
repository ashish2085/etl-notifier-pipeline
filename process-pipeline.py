import os
import json
import re
import datetime
import msal
import requests
import base64
import functions_framework
from contextlib import contextmanager
import csv
import io

from google.cloud import storage
from google.cloud.sql.connector import Connector, IPTypes
from google.oauth2 import service_account
import functions_framework

# Environment Variables
CLOUD_SQL_INSTANCE = os.getenv("CLOUD_SQL_INSTANCE", "your-cloudsql-instance-connection-name")
DB_NAME = os.getenv("DB_NAME", "your-database-name")
DB_USER = os.getenv("DB_USER", "your-db-username")
DB_PASSWORD = os.getenv("DB_PASSWORD", "your-db-password")
CUSTOM_SCHEMA = os.getenv("CUSTOM_SCHEMA", "your_custom_schema")
CONTROL_TABLE = f"{CUSTOM_SCHEMA}.processed_files"
DELETE_CONTROL_TABLE = f"{CUSTOM_SCHEMA}.delete_control"
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "your-json-credentials")

# how many rows per batch
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))


# Email settings (for sending approval emails)
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "sender_email")
PRODUCT_OWNER_EMAIL = os.getenv("PRODUCT_OWNER_EMAIL", "product_email")

@contextmanager
def db_connection():
    """
    Context manager to create and clean up the DB connection and connector.
    (You will obtain the connection once and pass it to functions.)
    """
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
        print(f"Error establishing database connection: {e}")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass
        connector.close()


def get_table_primary_key(conn, table_name):
    """Fetch primary key columns for the specified table."""
    schema, table = table_name.split(".")
    query = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary
    """
    cursor = conn.cursor()
    try:
        cursor.execute(query, (f"{schema}.{table}",))
        pk_columns = [row[0] for row in cursor.fetchall()]
        print(f"Primary keys for {table_name}: {pk_columns}")
        return pk_columns
    except Exception as e:
        print(f"Failed to fetch primary keys for {table_name}: {e}")
        raise
    finally:
        cursor.close()


def check_if_already_processed(conn, event_id):
    """Check if the file event is already processed."""
    query = f"SELECT is_processed FROM {CONTROL_TABLE} WHERE event_id = %s"
    cursor = conn.cursor()
    try:
        cursor.execute(query, (event_id,))
        result = cursor.fetchone()
        return (result is not None and result[0])
    except Exception as e:
        print(f"Error checking processed flag for event_id {event_id}: {e}")
        return False
    finally:
        cursor.close()


def create_table_if_not_exists(conn, table_name, csv_headers):
    """Create table if it does not exist, cloning from public table or creating new based on CSV headers."""
    schema, table = table_name.split(".")
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
        if cursor.fetchone()[0]:
            print(f"Table {table_name} exists")
            return

        cursor.execute("SELECT to_regclass(%s)", (f"public.{table}",))
        public_table = cursor.fetchone()[0]

        if public_table:
            print(f"Cloning structure from public.{table}")
            cursor.execute(f"CREATE TABLE {table_name} (LIKE public.{table} INCLUDING ALL)")
        else:
            print("Creating new table based on CSV headers")
            columns = ", ".join([f'"{header}" TEXT' for header in csv_headers])
            cursor.execute(f"CREATE TABLE {table_name} ({columns})")
        conn.commit()
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()


def get_table_schema(conn, table_name):
    """Fetch column names for the specified table."""
    schema, table = table_name.split(".")
    query = """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """
    cursor = conn.cursor()
    try:
        cursor.execute(query, (schema, table))
        columns = [row[0] for row in cursor.fetchall()]
        print(f"Fetched schema columns for {table_name}: {columns}")
        return columns
    except Exception as e:
        print(f"Failed to fetch schema for {table_name}: {e}")
        raise
    finally:
        cursor.close()

def insert_csv_data(conn, table_name, data, ordered_columns):
    quoted_columns = ", ".join([f'"{col}"' for col in ordered_columns])
    placeholders = ", ".join(["%s"] * len(ordered_columns))
    query = f"""
        INSERT INTO {table_name} ({quoted_columns})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
    """
    values = [tuple(row[col] for col in ordered_columns) for row in data]
    cursor = conn.cursor()
    try:
        cursor.executemany(query, values)
        conn.commit()
        msg = f"Inserted {len(data)} rows into {table_name}."
        print(msg)
        return msg
    except Exception as e:
        conn.rollback()
        err_msg = f"Insert error for table {table_name}: {e}"
        print(err_msg)
        raise Exception(err_msg)
    finally:
        cursor.close()


def update_csv_data(conn, table_name, data, ordered_columns):
    pk_columns = get_table_primary_key(conn, table_name)
    missing_pk = [pk for pk in pk_columns if pk not in ordered_columns]
    if missing_pk:
        raise ValueError(f"Missing primary keys in CSV: {missing_pk}")

    conflict_target = ", ".join([f'"{pk}"' for pk in pk_columns])
    update_cols = [col for col in ordered_columns if col not in pk_columns]
    update_assignments = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])
    quoted_columns = ", ".join([f'"{col}"' for col in ordered_columns])
    placeholders = ", ".join(["%s"] * len(ordered_columns))
    query = f"""
        INSERT INTO {table_name} ({quoted_columns})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_target}) DO UPDATE SET {update_assignments}
    """
    values = [tuple(row[col] for col in ordered_columns) for row in data]
    cursor = conn.cursor()
    try:
        cursor.executemany(query, values)
        conn.commit()
        msg = f"Upserted {len(data)} rows into {table_name}."
        print(msg)
        return msg
    except Exception as e:
        conn.rollback()
        err_msg = f"Update error for table {table_name}: {e}"
        print(err_msg)
        raise Exception(err_msg)
    finally:
        cursor.close()

def delete_csv_data(conn, table_name, data, ordered_columns):
    pk_columns = get_table_primary_key(conn, table_name)
    missing_pk = [pk for pk in pk_columns if pk not in ordered_columns]
    if missing_pk:
        raise ValueError(f"Missing primary keys in CSV: {missing_pk}")

    where_clause = " AND ".join([f'"{pk}" = %s' for pk in pk_columns])
    query = f"DELETE FROM {table_name} WHERE {where_clause}"
    deleted_count = 0
    cursor = conn.cursor()
    try:
        for row in data:
            values = [row[pk] for pk in pk_columns]
            cursor.execute(query, values)
            deleted_count += cursor.rowcount
        conn.commit()
        msg = f"Deleted {deleted_count} rows from {table_name}."
        print(msg)
        return msg
    except Exception as e:
        conn.rollback()
        err_msg = f"Delete error for table {table_name}: {e}"
        print(err_msg)
        raise Exception(err_msg)
    finally:
        cursor.close()



def get_next_query_id(conn):
    """Fetch the next available query ID from the delete control table."""
    query = f"SELECT COALESCE(MAX(QueryId), 0) FROM {DELETE_CONTROL_TABLE}"
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        max_id = cursor.fetchone()[0]
        next_id = max_id + 1
        print(f"Generated next QueryId: {next_id}")
        return next_id
    except Exception as e:
        print(f"Error generating next QueryId: {e}")
        raise
    finally:
        cursor.close()


def store_delete_queries(conn, table_name, rows, ordered_columns, event_id, approval_timestamp):
    """
    Store delete queries in the delete_control table for later processing.
    Before inserting a new delete query, check if the same query exists and has not been executed.
    If so, skip inserting that query.
    """
    # Get primary key(s) and validate CSV headers
    pk_columns = get_table_primary_key(conn, table_name)
    missing_pk = [pk for pk in pk_columns if pk not in ordered_columns]
    if missing_pk:
        raise ValueError(f"Missing primary keys in CSV: {missing_pk}")

    # Retrieve the starting query id for new entries
    next_query_id = get_next_query_id(conn)
    cursor = conn.cursor()
    inserted_queries = 0  # To keep track of how many queries are inserted successfully

    try:
        for i, row in enumerate(rows):
            query_id = next_query_id + i

            # Construct the WHERE clause based on primary key columns from the current row
            where_clauses = []
            for pk in pk_columns:
                # Here we quote the pk name and wrap the value in single quotes.
                value = row[pk]
                where_clauses.append(f'"{pk}" = \'{value}\'')
            where_clause = " AND ".join(where_clauses)
            delete_query = f"DELETE FROM {table_name} WHERE {where_clause}"

            # Check if the same delete query already exists with ExecutedFlag set to False
            check_query = f"""
                SELECT COUNT(*) 
                FROM {DELETE_CONTROL_TABLE} 
                WHERE DeleteQuery = %s AND ExecutedFlag = FALSE
            """
            cursor.execute(check_query, (delete_query,))
            count = cursor.fetchone()[0]
            if count > 0:
                # A matching, not-yet-executed delete query exists. Skip insertion.
                print(f"Delete query already exists and is pending execution. Skipping insertion: {delete_query}")
                continue

            # Insert a new record into the delete control table
            insert_query = f"""
                INSERT INTO {DELETE_CONTROL_TABLE}
                (QueryId, EventId, DeleteQuery, DeleteFlag, ExecutedFlag, DeletedApprovalTimestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            # Using True for DeleteFlag and False for ExecutedFlag as specified.
            cursor.execute(insert_query, (query_id, event_id, delete_query, True, False, approval_timestamp))
            inserted_queries += 1

        conn.commit()
        print(f"Stored {inserted_queries} delete queries into {DELETE_CONTROL_TABLE}")
    except Exception as e:
        print(f"Error storing delete queries for table {table_name}: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

def process_file_operation(conn, operation, bucket, file_name, table_name, event_id, approval_timestamp):
    try:
        # Download and parse file from GCS (same as before)
        credentials_info = json.loads(GOOGLE_APPLICATION_CREDENTIALS)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        storage_client = storage.Client(credentials=credentials)
        bucket_obj = storage_client.bucket(bucket)
        blob = bucket_obj.blob(file_name)
        content = blob.download_as_text()
        print(f"Downloaded file {file_name} from bucket {bucket}.")
    except Exception as e:
        err = f"Failed to download file {file_name} from bucket {bucket}: {e}"
        print(err)
        raise Exception(err)

    rows = []
    csv_reader = csv.DictReader(io.StringIO(content))
    if not csv_reader.fieldnames:
        err = "CSV file is empty or missing headers."
        print(err)
        raise Exception(err)

    for row in csv_reader:
        rows.append(row)
    print(f"Parsed {len(rows)} rows from file {file_name} with headers: {list(rows[0].keys())}")

    ordered_columns = list(rows[0].keys())

    try:
        if operation != "delete":
            create_table_if_not_exists(conn, table_name, ordered_columns)
        # Execute operation and capture result message
        if operation == "insert":
            result = insert_csv_data(conn, table_name, rows, ordered_columns)
        elif operation == "update":
            result = update_csv_data(conn, table_name, rows, ordered_columns)
        elif operation == "delete":
            result = store_delete_queries(conn, table_name, rows, ordered_columns, event_id, approval_timestamp)
            result = f"Stored delete queries for {len(rows)} rows into {DELETE_CONTROL_TABLE}."
        else:
            err = f"Unsupported operation: {operation}"
            print(err)
            raise Exception(err)
        return result
    except Exception as e:
        err = f"Error during '{operation}' operation on {table_name}: {e}"
        print(err)
        raise Exception(err)

def notify_operation_result(operation: str, table_name: str, file_name: str, event_id: str, bucket: str, details: str, success: bool, timestamp: str) -> None:
    """
    Sends an email to notify the result of a data operation (insert/update/delete) with details.
    """
    status = "Success" if success else "Failure"
    subject = f"{status}: {operation.capitalize()} Operation on {table_name}"
    body_html = f"""
    <html>
      <body>
        <p>Dear User,</p>
        <p>The <strong>{operation}</strong> operation on table <strong>{table_name}</strong> via file <strong>{file_name}</strong> (Event ID: {event_id}) has finished processing.</p>
        <p><strong>Status:</strong> {status}</p>
        <p><strong>Details:</strong></p>
        <p>{details}</p>
        <p><strong>Bucket:</strong> {bucket}</p>
        <p><strong>Timestamp:</strong> {timestamp}</p>
        <p>Best regards,<br>Your Data Engineering Team</p>
      </body>
    </html>
    """
    # Sending email to the intended recipient. You can customize the recipient as needed.
    send_email(SENDER_EMAIL, PRODUCT_OWNER_EMAIL, subject, body_html)

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


@functions_framework.cloud_event
def process_approval_message(cloud_event):
    try:
        pubsub_message = cloud_event.data.get("message", {})
        if not pubsub_message:
            print("No Pub/Sub message found in event data. Skipping processing.")
            return

        message_data_b64 = pubsub_message.get("data", "")
        if not message_data_b64.strip():
            print("Empty Pub/Sub message data. Skipping processing.")
            return

        payload = json.loads(base64.b64decode(message_data_b64).decode("utf-8"))
        print("Received payload:", payload)

        event_id = payload.get("event_id")
        action = (payload.get("action") or "").lower()
        file_name = payload.get("file_name")
        table_name = payload.get("table_name")
        operation = (payload.get("operation") or "").lower()
        bucket = payload.get("bucket")
        approval_timestamp = payload.get("approval_timestamp", datetime.datetime.utcnow().isoformat() + "Z")

        if not event_id or not action:
            print("Missing required fields (event_id or action) in payload.")
            return

        with db_connection() as conn:
            if check_if_already_processed(conn, event_id):
                print(f"Event {event_id} already processed. Skipping duplicate processing.")
                return

            new_status = "approved" if action == "approve" else "rejected" if action == "reject" else None
            if new_status is None:
                print(f"Unknown action received: {action}")
                return

            print(f"Processing event {event_id} with action '{new_status}'.")

            if new_status == "approved":
                if not all([file_name, bucket, table_name, operation]):
                    print("Missing file operation details; cannot proceed with processing.")
                    return

                print(f"Performing '{operation}' on table {table_name} using file {file_name} from bucket {bucket}.")
                try:
                    # Capture the result detail message
                    operation_result = process_file_operation(conn, operation, bucket, file_name, table_name, event_id, approval_timestamp)
                    # Mark the control record with success status
                    update_query = f"""
                        UPDATE {CONTROL_TABLE}
                        SET status = %s,
                            approval_timestamp = %s,
                            is_processed = TRUE
                        WHERE event_id = %s
                    """
                    cursor = conn.cursor()
                    cursor.execute(update_query, (new_status, approval_timestamp, event_id))
                    conn.commit()
                    cursor.close()
                    
                    # Notify user about the successful operation
                    notify_operation_result(operation, table_name, file_name, event_id, bucket, operation_result, True, approval_timestamp)
                    print(f"Control record for event {event_id} updated to '{new_status}' and notification sent.")
                except Exception as op_ex:
                    error_detail = str(op_ex)
                    # Update control record with failure status (if needed)
                    update_query = f"""
                        UPDATE {CONTROL_TABLE}
                        SET status = %s,
                            approval_timestamp = %s,
                            is_processed = TRUE
                        WHERE event_id = %s
                    """
                    cursor = conn.cursor()
                    cursor.execute(update_query, ("failed", approval_timestamp, event_id))
                    conn.commit()
                    cursor.close()
                    
                    # Notify user about the failure
                    notify_operation_result(operation, table_name, file_name, event_id, bucket, error_detail, False, approval_timestamp)
                    print(f"Operation for event {event_id} failed: {error_detail}")
                    return
            else:
                # For rejected action update the control record without performing any file operations.
                update_query = f"""
                    UPDATE {CONTROL_TABLE}
                    SET status = %s,
                        approval_timestamp = %s,
                        is_processed = TRUE
                    WHERE event_id = %s
                """
                cursor = conn.cursor()
                cursor.execute(update_query, (new_status, approval_timestamp, event_id))
                conn.commit()
                cursor.close()
                print(f"Control record for event {event_id} updated to '{new_status}'.")
    except Exception as ex:
        evt_id = payload.get("event_id", "") if 'payload' in locals() else ""
        print(f"Failed to process Pub/Sub message for event {evt_id}: {ex}")
        return

# Requirement.txt
# functions-framework==3.*
# cloud-sql-python-connector
# pandas
# google-cloud-storage
# google-auth
# pg8000
# msal
# dask[complete]
# gcsfs
# numpy