from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from data_processor import DataProcessor
from time_series import ProphetForecaster


default_args = {
    "owner": "airflow",  # Who owns/maintains this DAG
    "depends_on_past": False,  # Tasks don't depend on past runs
    "start_date": datetime(2024, 9, 23),  # When the DAG should start running
    "email_on_failure": False,  # Don't send emails on task failure
    "email_on_retry": False,  # Don't send emails on task retries
    "retries": 1,  # Number of times to retry a failed task
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes between retries
}


# Initialize Spark Session
def create_spark_session():
    load_dotenv()
    spark = (
        SparkSession.builder.appName("CandyStoreAnalytics")
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.mongodb.input.uri", os.getenv("MONGODB_URI"))
        .getOrCreate()
    )
    return spark


# Load Configuration
def get_date_range(start_date: str, end_date: str) -> list[str]:
    """Generate a list of dates between start and end date"""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    date_list = []

    current = start
    while current <= end:
        date_list.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    return date_list


def load_config():
    """Load configuration from environment variables"""
    return {
        "mongodb_uri": os.getenv("MONGODB_URI"),
        "mongodb_db": os.getenv("MONGO_DB"),
        "mongodb_collection_prefix": os.getenv("MONGO_COLLECTION_PREFIX"),
        "mysql_url": os.getenv("MYSQL_URL"),
        "mysql_user": os.getenv("MYSQL_USER"),
        "mysql_password": os.getenv("MYSQL_PASSWORD"),
        "mysql_db": os.getenv("MYSQL_DB"),
        "customers_table": os.getenv("CUSTOMERS_TABLE"),
        "products_table": os.getenv("PRODUCTS_TABLE"),
        "output_path": os.getenv("OUTPUT_PATH"),
        "reload_inventory_daily": os.getenv("RELOAD_INVENTORY_DAILY", "false").lower()
        == "true",
    }


def set_config():
    load_dotenv()
    config = load_config()
    date_range = get_date_range(
        os.getenv("MONGO_START_DATE"), os.getenv("MONGO_END_DATE")
    )
    return config, date_range


# Load Data
def load_data():
    spark = create_spark_session()
    config, date_range = set_config()
    data_processor = DataProcessor(spark)
    data_processor.configure(config)

    # Load MySQL & MongoDB data
    data_processor.load_customers()
    data_processor.load_products()
    data_processor.set_initial_inventory()
    data_processor.load_mongo(config, date_range)
    print("Loaded data:", data_processor.products_df)  # Check if data is loaded


# Process Data
def process_data():
    spark = create_spark_session()
    config, date_range = set_config()
    data_processor = DataProcessor(spark)
    data_processor.configure(config)
    data_processor.load_customers()
    data_processor.load_products()
    data_processor.set_initial_inventory()
    data_processor.load_mongo(config, date_range)
    updated_inventory_df = data_processor.update_inventory_table()
    updated_inventory_df = data_processor.sort_orders(updated_inventory_df)
    data_processor.save_outputs(config, updated_inventory_df)


# Generate Forecasts
def generate_forecast():
    spark = create_spark_session()
    config, date_range = set_config()
    data_processor = DataProcessor(spark)
    data_processor.configure(config)
    data_processor.load_customers()
    data_processor.load_products()
    data_processor.set_initial_inventory()
    data_processor.load_mongo(config, date_range)
    forecast_df = data_processor.forecast_sales_and_profits(
        data_processor.daily_summary_df
    )
    forecast_df = data_processor.format_values(forecast_df)
    if forecast_df is not None:
        data_processor.save_to_csv(
            forecast_df, os.getenv("OUTPUT_PATH"), "sales_profit_forecast.csv"
        )


# Cleanup
def cleanup():
    spark = create_spark_session()
    spark.stop()


# Define DAG
with DAG(
    "candy_store_batch_dag",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Runs daily at 6 AM
    catchup=False,
) as dag:

    load_data_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    process_data_task = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    generate_forecast_task = PythonOperator(
        task_id="generate_forecast",
        python_callable=generate_forecast,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup,
    )

    # Task Dependencies
    load_data_task >> process_data_task >> generate_forecast_task >> cleanup_task
