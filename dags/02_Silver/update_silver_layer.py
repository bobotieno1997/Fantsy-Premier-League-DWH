# Import all relevant libraries
import psycopg2
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import logging
from airflow.models import Variable

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# DAG default arguments
default_args = {
    "owner": "Fantasy Premier League",
    "depends_on_past": False,
    "start_date": datetime(2023, 2, 7),
    "email": ["bobotieno99@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Define Airflow DAG
with DAG(
    "FPL_silver_tables",
    default_args=default_args,
    schedule="10 0 * * *",
    catchup=False,  # Set to False to avoid running all past instances
    tags=["FPL", "Silver"],
) as dag:

    @task(retries=3, retry_delay=timedelta(minutes=1))
    def update_fpl_silver_layer():
        """Update the silver layer by executing stored procedures."""
        
        # Set up database connection parameters
        env_connection = Variable.get("fantasypl", deserialize_json=True)

        # Validate environment variables
        if not all(env_connection.values()):
            logging.error("❌ Missing database connection parameters from Airflow environment")
            raise ValueError("Missing database connection parameters")

        config_params = {
            'dbname': env_connection['dbname'],
            'user': env_connection['user'],
            'password': env_connection['password'],
            'host': env_connection['host'],
            'port': env_connection['port']
        }

        # Establish database connection
        try:
            connection = psycopg2.connect(**config_params)
            cur = connection.cursor()
            logging.info("✅ Database connection established successfully")
        except Exception as e:
            logging.error(f"❌ Failed to connect to database: {e}")
            raise

        # SQL command to update silver layer
        update_silver_layer = """ 
            CALL silver.usp_update_team_info();
            CALL silver.usp_update_player_info();
            CALL silver.usp_update_games_info();
            CALL silver.usp_update_future_games_info();
            CALL silver.usp_update_players_stats();
        """

        # Execute stored procedures
        try:
            cur.execute(update_silver_layer)
            connection.commit()
            logging.info("✅ Silver layer updated successfully")
        except Exception as e:
            logging.error(f"❌ Error updating silver layer: {e}")
            connection.rollback()
            raise
        finally:
            # Clean up resources
            try:
                if cur:
                    cur.close()
                if connection:
                    connection.close()
                logging.info("✅ Database connection closed")
            except Exception as e:
                logging.error(f"❌ Error closing database connection: {e}")
                raise

    # Execute the task
    update_fpl_silver_layer()

        