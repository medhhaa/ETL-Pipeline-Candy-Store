from pyspark.sql import SparkSession, DataFrame
from data_processor import DataProcessor
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
from pyspark.sql.functions import col
from typing import Dict, Tuple
import traceback


def create_spark_session(app_name: str = "CandyStoreAnalytics") -> SparkSession:
    """
    Create and configure Spark session with MongoDB and MySQL connectors
    """
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.mongodb.input.uri", os.getenv("MONGODB_URI"))
        .getOrCreate()
    )


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


def print_header():
    print("*" * 80)
    print("                        CANDY STORE DATA PROCESSING SYSTEM")
    print("                               Analysis Pipeline")
    print("*" * 80)


def print_processing_period(date_range: list):
    print("\n" + "=" * 80)
    print("PROCESSING PERIOD")
    print("-" * 80)
    print(f"Start Date: {date_range[0]}")
    print(f"End Date:   {date_range[-1]}")
    print("=" * 80)


def setup_configuration() -> Tuple[Dict, list]:
    """Setup application configuration"""
    load_dotenv()
    config = load_config()
    date_range = get_date_range(
        os.getenv("MONGO_START_DATE"), os.getenv("MONGO_END_DATE")
    )
    return config, date_range


def load_config() -> Dict:
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


def initialize_data_processor(spark: SparkSession, config: Dict) -> DataProcessor:
    """Initialize and configure the DataProcessor"""
    print("\nINITIALIZING DATA SOURCES")
    print("-" * 100)

    data_processor = DataProcessor(spark)
    data_processor.config = config
    return data_processor


# new: helper function to just better output
def print_database_load_name(database: int) -> None:
    """Print Database Loading Message"""
    print(f"\nLOAD DATA FROM {database}:")
    print("-" * 100)


def print_processing_complete(total_cancelled_items: int) -> None:
    """Print processing completion message"""
    print("\nPROCESSING COMPLETE")
    print("=" * 100)
    print(f"Total Cancelled Items: {total_cancelled_items}")


def print_daily_summary(orders_df, order_items_df, cancelled_count):
    """Print summary of daily processing"""
    processed_items = order_items_df.filter(col("quantity") > 0).count()
    print("\nDAILY PROCESSING SUMMARY")
    print("-" * 40)
    print(f"• Successfully Processed Orders: {orders_df.count()}")
    print(f"• Successfully Processed Items: {processed_items}")
    print(f"• Items Cancelled (Inventory): {cancelled_count}")
    print("-" * 40)


def generate_forecasts(
    data_processor: DataProcessor, final_daily_summary, output_path: str
):
    """Generate and save sales forecasts"""
    print("\nGENERATING FORECASTS")
    print("-" * 80)

    try:
        if final_daily_summary is not None and final_daily_summary.count() > 0:
            print("Schema before forecasting:", final_daily_summary.printSchema())
            forecast_df = data_processor.forecast_sales_and_profits(final_daily_summary)
            if forecast_df is not None:
                data_processor.save_to_csv(
                    forecast_df, output_path, "sales_profit_forecast.csv"
                )
        else:
            print("Warning: No daily summary data available for forecasting")
    except Exception as e:
        print(f"⚠️  Warning: Could not generate forecasts: {str(e)}")
        print("Stack trace:", traceback.format_exc())


def main():
    print_header()

    # Setup
    config, date_range = setup_configuration()
    print_processing_period(date_range)

    # Initialize processor
    spark = create_spark_session()
    data_processor = DataProcessor(spark)

    try:
        # Configure and load data
        data_processor.configure(config)
        """ 1. Push data to sources (MySQL and MongoDB)
            1.1 Load data into the Database
            a. Customers data: execute only once. """
        # data_processor.save_csv_to_mysql(
        #     config["mysql_url"],
        #     config["mysql_user"],
        #     config["mysql_password"],
        #     config["customers_table"],
        #     "data/dataset_22/customers.csv"
        # )

        """ b. Products data: execute only once. """
        # data_processor.save_csv_to_mysql(
        #     config["mysql_url"],
        #     config["mysql_user"],
        #     config["mysql_password"],
        #     config["products_table"],
        #     "data/dataset_22/products.csv"
        # )

        # 1.2 Load data from sources:
        print_database_load_name("MYSQL")
        # a. Customers Data from MySQL
        customers_df = data_processor.load_mysql_data(
            config["mysql_url"],
            config["customers_table"],
            config["mysql_user"],
            config["mysql_password"],
        )
        print("Customer Data:")
        customers_df.show(5)
        customers_df.printSchema()
        print(f"Dimensions: {customers_df.count()} x {len(customers_df.columns)} \n")

        # b. Products Data from MySQL
        products_df = data_processor.load_mysql_data(
            config["mysql_url"],
            config["products_table"],
            config["mysql_user"],
            config["mysql_password"],
        )
        print("Product Data:")
        products_df.show(5)
        products_df.printSchema()
        print(f"Dimensions: {products_df.count()} x {len(products_df.columns)} \n")

        print_database_load_name("MONGO DB")
        # 1.3 Load mongodb data in 10 dfs and display them
        transaction_dataframes = {}
        for i, date_str in enumerate(date_range):
            # Construct collection name dynamically
            collection_name = f"{config['mongodb_collection_prefix']}{date_str}" # print(config["mongodb_uri"], config["mongodb_db"]) # THE DEBUG LINE THAT SAVED MY MONGODB IMPORT:
            try:
                # Load the MongoDB collection into a DataFrame
                print(f"Collection - {collection_name}:")
                df = data_processor.load_mongo_data(
                    config["mongodb_uri"], config["mongodb_db"], collection_name
                )
                df.show(5)
                df.printSchema()
                # Display Dimensions
                print(f"Dimensions: {df.count()} x {len(df.columns)} \n")
                transaction_dataframes[f"transaction_{i+1}"] = df

            except Exception as e:
                print(f"Failed to load data for {collection_name}: {e}")

        print("Start batch processing for project 2!")

        # """2. BATCH PROCESSING: 
        # Transform transaction data into the orders table and order_line_items table.
        # Extract orders and order_line_items from transactions."""
        data_processor.initialize_products(products_df)
        for name, transaction_df in transaction_dataframes.items():
            data_processor.process(transaction_df)
        # print("Orders DF:")
        # orders_df = data_processor.process_orders(
        #     customers_df, products_df
        # )

        # print("Order Line DF:")
        # order_line_df = data_processor.process_order_line_df(
        #      products_df
        # )
        # # Save to CSV files!
        # data_processor.save_to_csv(orders_df, config["output_path"], "orders.csv")
        # data_processor.save_to_csv(
        #     order_line_df, config["output_path"], "order_line_items.csv"
        # )

        # print("Daily Summary:")
        # daily_summary_df = data_processor.create_daily_summary(
        #     orders_df, products_df
        # )
        # data_processor.save_to_csv(
        #     daily_summary_df, config["output_path"], "daily_summary.csv"
        # )

        # Generate forecasts
        try:
            # daily_summary_df follows the same schema as the daily_summary that you save to csv
            # schema:
            # - date: date - The business date
            # - num_orders: integer - Total number of orders for the day
            # - total_sales: decimal(10,2) - Total sales amount for the day
            # - total_profit: decimal(10,2) - Total profit for the day
            forecast_df = data_processor.forecast_sales_and_profits(daily_summary_df)
            if forecast_df is not None:
                data_processor.save_to_csv(
                    forecast_df, config["output_path"], "sales_profit_forecast.csv"
                )
        except Exception as e:
            print(f"⚠️  Warning: Could not generate forecasts: {str(e)}")

    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        raise
    finally:
        print("\nCleaning up...")
        spark.stop()


if __name__ == "__main__":
    main()
