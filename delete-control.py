import os
import json
from google.cloud.sql.connector import Connector, IPTypes
from google.oauth2 import service_account
import functions_framework

# Environment Variables (set these in your function’s configuration)
CLOUD_SQL_INSTANCE = os.getenv("CLOUD_SQL_INSTANCE", "your-cloudsql-instance-connection-name")
DB_NAME = os.getenv("DB_NAME", "your-database-name")
DB_USER = os.getenv("DB_USER", "your-db-username")
DB_PASSWORD = os.getenv("DB_PASSWORD", "your-db-password")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "your-json-credentials")
CUSTOM_SCHEMA = os.getenv("CUSTOM_SCHEMA", "your_custom_schema")
DELETE_CONTROL_TABLE = f"{CUSTOM_SCHEMA}.delete_control"

def create_connection():
    """
    Creates and returns a tuple (connection, connector) for the Cloud SQL database.
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
        return conn, connector
    except Exception as e:
        print(f"Error creating database connection: {e}")
        raise

@functions_framework.http
def execute_pending_deletes(request):
    """
    Cloud Function that retrieves pending delete queries from the delete control table,
    executes them, and updates each control record with an executed flag and timestamp.
    
    Trigger this function via HTTP (or schedule it with Cloud Scheduler) to run daily.
    """
    conn, connector = None, None
    cursor = None
    try:
        conn, connector = create_connection()
        cursor = conn.cursor()

        # Retrieve pending delete queries (rows where ExecutedFlag is false)
        select_query = f"""
            SELECT QueryId, DeleteQuery 
              FROM {DELETE_CONTROL_TABLE} 
             WHERE ExecutedFlag = false
        """
        cursor.execute(select_query)
        pending_rows = cursor.fetchall()
        print(f"Found {len(pending_rows)} pending delete queries.")

        if not pending_rows:
            return ("No pending delete queries to execute.", 200)

        for row in pending_rows:
            query_id, delete_query = row
            print(f"Executing delete query for QueryId {query_id}: {delete_query}")

            try:
                # Execute the dynamic DELETE query stored in the control table.
                cursor.execute(delete_query)
                
                # Update the control record to mark this query as executed.
                update_query = f"""
                    UPDATE {DELETE_CONTROL_TABLE}
                       SET ExecutedFlag = true,
                           ExecutedDeleteTimestamp = NOW()
                     WHERE QueryId = %s
                """
                cursor.execute(update_query, (query_id,))
                print(f"QueryId {query_id} executed and control record updated.")
            except Exception as e:
                print(f"Error executing delete query for QueryId {query_id}: {e}")
                # Optionally log or send notification about the failure.
                continue

        conn.commit()
        return (f"Executed {len(pending_rows)} pending delete queries.", 200)

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error in execute_pending_deletes: {e}")
        return (f"Error executing pending deletes: {e}", 500)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        if connector:
            connector.close()

#Requirement.txt
# functions-framework==3.*
# cloud-sql-python-connector
# psycopg2-binary
# pandas
# google-cloud-storage
# google-auth
# pg8000
# msal
# Flask