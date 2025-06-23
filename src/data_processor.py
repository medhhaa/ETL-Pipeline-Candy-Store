from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    explode,
    col,
    round as spark_round,
    sum as spark_sum,
    count,
    abs as spark_abs,
    when
)
from typing import Dict, Tuple
import os
import glob
import shutil
import decimal
import numpy as np
from time_series import ProphetForecaster
from datetime import datetime, timedelta
from pyspark.sql.types import DoubleType, DecimalType


class DataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        # Initialize all class properties
        self.config = None
        self.current_inventory = None
        self.inventory_initialized = False
        self.original_products_df = None  # Store original products data
        self.reload_inventory_daily = False  # New flag for inventory reload
        self.order_items = None
        self.products_df = None
        self.customers_df = None
        self.transactions_df = None
        self.orders_df = None
        self.order_line_items_df = None
        self.daily_summary_df = None
        self.total_cancelled_items = 0
        self.joined_df = None # added

    def configure(self, config: Dict) -> None:
        """Configure the data processor with environment settings"""
        self.config = config
        self.reload_inventory_daily = config.get("reload_inventory_daily", False)
        print("\nINITIALIZING DATA SOURCES")
        print("-" * 80)
        if self.reload_inventory_daily:
            print("Daily inventory reload: ENABLED")
        else:
            print("Daily inventory reload: DISABLED")

    # Add data to MySQL and MongoDB:
    def save_csv_to_mysql(
        self,
        jdbc_url: str,
        db_user: str,
        db_password: str,
        table_name: str,
        csv_file_path: str,
    ) -> None:
        """
        Save CSV data into a MySQL table using Spark.
        :param jdbc_url: JDBC URL for the MySQL database
        :param db_user: Database username
        :param db_password: Database password
        :param table_name: MySQL table name
        :param csv_file_path: Path to the CSV file
        """
        try:
            # Load the CSV file into a DataFrame
            df = self.spark.read.option("header", "true").csv(csv_file_path)

            # Save the DataFrame to the MySQL table using the 'overwrite' mode (use 'append' to add data)
            df.write.format("jdbc").option("url", jdbc_url).option(
                "driver", "com.mysql.cj.jdbc.Driver"
            ).option("dbtable", table_name).option("user", db_user).option(
                "password", db_password
            ).mode(
                "overwrite"
            ).save()

            print(f"Data successfully saved to MySQL table '{table_name}'")
            # print(f"CSV data from {csv_file_path} saved to MySQL table {table_name}")
        except Exception as e:
            print(f"Error saving CSV to MySQL: {str(e)}")

    # Load data from MySQL and MongoDB:
    def load_mysql_data(
        self, jdbc_url: str, db_table: str, db_user: str, db_password: str
    ) -> DataFrame:
        """
        Load data from MySQL database.

        :param jdbc_url: JDBC URL for the MySQL database
        :param db_table: Name of the table to load data from
        :param db_user: Database username
        :param db_password: Database password
        :return: DataFrame containing the loaded MySQL data
        """
        return (
            self.spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", db_table)
            .option("user", db_user)
            .option("password", db_password)
            .load()
        )

    def load_mongo_data(
        self, mongodb_uri, db_name: str, collection_name: str
    ) -> DataFrame:
        """
        Load data from MongoDB.

        :param db_name: Name of the MongoDB database
        :param collection_name: Name of the collection to load data from
        :return: DataFrame containing the loaded MongoDB data
        """
        # return (
        #     self.spark.read.format("mongo")
        #     .option("database", db_name)
        #     .option("collection", collection_name)
        #     .load()
        # )
        return (
            self.spark.read.format("com.mongodb.spark.sql.DefaultSource")
            .option("spark.mongodb.input.uri", mongodb_uri)
            .option("spark.mongodb.input.database", db_name)
            .option("spark.mongodb.input.collection", collection_name)
            .load()
        )


    def explode_transactions(self, transactions_main):
        """
        Explodes the 'items' array in transactions DataFrame to get each product as a separate row for better function complexity.
        This can be used by process_orders and process_order_line.
        Filters out rows with null quantity or product_id.

        Args:
        transactions_df: DataFrame containing transaction data

        Returns:
        exploded_df: DataFrame with exploded 'items' and filtered out null quantities
        """
        
        # Explode 'items' in transactions DataFrame to get each product as a separate row
        transactions_main = transactions_main.withColumn(
            "item", F.explode(transactions_main["items"])
        )

        # Select necessary columns and ensure no null quantity or product_id
        exploded_df = transactions_main.select(
            "transaction_id",
            "customer_id",
            "timestamp",
            F.col("item.product_id").alias("product_id"),
            F.col("item.product_name").alias("product_name"),
            F.col("item.qty").alias("quantity"),
        ).filter(F.col("quantity").isNotNull())

        # Initialize transactions_df if it is None (first iteration)
        if self.transactions_df is None:
            self.transactions_df = exploded_df
        else:
            # Append to the existing DataFrame by union
            self.transactions_df = self.transactions_df.unionByName(exploded_df)
        # print("Dimensions FROM DATAPROCESSOR: ", self.transactions_df.count())
        return exploded_df

    def process_order_line_df(self, products_df):
        """
        Generates the 'order_line_items' DataFrame from transactions and product data.
        - Explodes items, calculates line total for each item.

        Args:
        products_df: DataFrame containing product data

        Returns:
        order_line_items_df: DataFrame with order line details
        """

        # Join with products to get the unit price (sales_price) and calculate line total in a single step
        order_line_items_df = (
            self.transactions_df.join(
                products_df,
                self.transactions_df["product_id"] == products_df["product_id"],
                "inner",
            )
            .select(
                F.col("transaction_id").alias("order_id"),
                self.transactions_df.product_id,
                "quantity",
                F.col("sales_price")
                .cast("float")
                .alias("unit_price"),  # Cast sales_price to float
            )
            .filter(
                F.col("unit_price").isNotNull()
            )  # Filter out rows with null unit_price
            .withColumn(
                "line_total", F.round(F.col("quantity") * F.col("unit_price"), 2)
            )  # Calculate line_total
            .select(
                "order_id", "product_id", "quantity", "unit_price",
                F.format_number("line_total", 2).alias("line_total"), 
            )  # Select final columns
            .orderBy("order_id", "product_id")  # Sort by order_id and product_id
        )

        # Show first 5 rows of the result
        order_line_items_df.show(5)

        return order_line_items_df

    def create_daily_summary(self, orders_df, products_df):
        """
        Creates a daily summary table that includes:
        - Date
        - Number of orders
        - Total sales (total amount of orders)
        - Total profit (total sales - total cost)

        Args:
        orders_df: DataFrame containing orders data (including order details like order_datetime, total_amount)
        products_df: DataFrame containing product data (including sales_price and cost_to_make)

        Returns:
        daily_summary_df: DataFrame containing daily order statistics
        """
        # Join with products to get sales price (unit price) and ensure no null sales_price
        transactions_with_products_df = (
            self.transactions_df.join(
                products_df,
                self.transactions_df["product_id"] == products_df["product_id"],
                "left",
            )
            .select(
                "transaction_id",
                "customer_id",
                "timestamp",
                self.transactions_df.product_id,
                self.transactions_df.product_name,
                "quantity",
                F.col("sales_price")
                .cast("float")
                .alias("sales_price"),  # Cast to float
            )
            .filter(
                F.col(
                    "sales_price"
                ).isNotNull()  # Filter out rows where sales_price is null
            )
        )
        # Assuming 'timestamp' column is in the format 'yyyy-MM-ddTHH:mm:ss.SSSSSS'
        orders_df = orders_df.withColumn(
            "datetime",
            F.to_timestamp(F.col("order_datetime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
        )
        # Extract order_date from order_datetime
        orders_df = orders_df.withColumn(
            "date", F.to_date(F.col("datetime"), "yyyy-MM-dd")
        )

        # Join orders_df with transactions_with_products_df to retain product_id
        orders_with_products_df = orders_df.join(
            transactions_with_products_df,
            orders_df["order_id"] == transactions_with_products_df["transaction_id"],
            "left",
        )

        # Join with products_df to get cost and sales_price
        orders_with_cost_df = orders_with_products_df.join(
            products_df,
            orders_with_products_df["product_id"] == products_df["product_id"],
            "left",
        ).select(
            ("date"),
            "total_amount",  # Total sales per order
            F.col("cost_to_make").alias("cost"),  # Cost per product
            orders_with_products_df.product_id,
            "quantity",
        )

        # Calculate total sales, total profit, and number of orders per day
        daily_summary_df = (
            orders_with_cost_df.groupBy("date")
            .agg(
                F.count("total_amount").alias("num_orders"),  # Number of orders per day
                F.round(F.sum("total_amount"), 2).alias(
                    "total_sales"
                ),  # Total sales per day
                F.round(
                    F.sum(F.col("total_amount") - F.col("cost") * F.col("quantity")), 2
                ).alias(
                    "total_profit"
                ),  # Total profit per day
            )
            .orderBy("date")
        )

        # Show the daily summary table
        daily_summary_df.show(truncate=False)

        return daily_summary_df

    def finalize_processing(self) -> None:
        """Finalize processing and create summary"""
        print("\nPROCESSING COMPLETE")
        print("=" * 80)
        print(f"Total Cancelled Items: {self.total_cancelled_items}")

    # ------------------------------------------------------------------------------------------------
    # Try not to change the logic of the time series forecasting model
    # DO NOT change functions with prefix _
    # ------------------------------------------------------------------------------------------------
    def forecast_sales_and_profits(
        self, daily_summary_df: DataFrame, forecast_days: int = 1
    ) -> DataFrame:
        """
        Main forecasting function that coordinates the forecasting process
        """
        try:
            # Build model
            model_data = self.build_time_series_model(daily_summary_df)

            # Calculate accuracy metrics
            metrics = self.calculate_forecast_metrics(model_data)

            # Generate forecasts
            forecast_df = self.make_forecasts(model_data, forecast_days)

            return forecast_df

        except Exception as e:
            print(
                f"Error in forecast_sales_and_profits: {str(e)}, please check the data"
            )
            return None

    def print_inventory_levels(self) -> None:
        """Print current inventory levels for all products"""
        print("\nCURRENT INVENTORY LEVELS")
        print("-" * 40)

        inventory_data = self.current_inventory.orderBy("product_id").collect()
        for row in inventory_data:
            print(
                f"• {row['product_name']:<30} (ID: {row['product_id']:>3}): {row['current_stock']:>4} units"
            )
        print("-" * 40)

    def build_time_series_model(self, daily_summary_df: DataFrame) -> dict:
        """Build Prophet models for sales and profits"""
        print("\n" + "=" * 80)
        print("TIME SERIES MODEL CONSTRUCTION")
        print("-" * 80)

        model_data = self._prepare_time_series_data(daily_summary_df)
        return self._fit_forecasting_models(model_data)

    def calculate_forecast_metrics(self, model_data: dict) -> dict:
        """Calculate forecast accuracy metrics for both models"""
        print("\nCalculating forecast accuracy metrics...")

        # Get metrics from each model
        sales_metrics = model_data["sales_model"].get_metrics()
        profit_metrics = model_data["profit_model"].get_metrics()

        metrics = {
            "sales_mae": sales_metrics["mae"],
            "sales_mse": sales_metrics["mse"],
            "profit_mae": profit_metrics["mae"],
            "profit_mse": profit_metrics["mse"],
        }

        # Print metrics and model types
        print("\nForecast Error Metrics:")
        print(f"Sales Model Type: {sales_metrics['model_type']}")
        print(f"Sales MAE: ${metrics['sales_mae']:.2f}")
        print(f"Sales MSE: ${metrics['sales_mse']:.2f}")
        print(f"Profit Model Type: {profit_metrics['model_type']}")
        print(f"Profit MAE: ${metrics['profit_mae']:.2f}")
        print(f"Profit MSE: ${metrics['profit_mse']:.2f}")

        return metrics

    def make_forecasts(self, model_data: dict, forecast_days: int = 7) -> DataFrame:
        """Generate forecasts using Prophet models"""
        print(f"\nGenerating {forecast_days}-day forecast...")

        forecasts = self._generate_model_forecasts(model_data, forecast_days)
        forecast_dates = self._generate_forecast_dates(
            model_data["training_data"]["dates"][-1], forecast_days
        )

        return self._create_forecast_dataframe(forecast_dates, forecasts)

    def _prepare_time_series_data(self, daily_summary_df: DataFrame) -> dict:
        """Prepare data for time series modeling"""
        data = (
            daily_summary_df.select("date", "total_sales", "total_profit")
            .orderBy("date")
            .collect()
        )

        dates = np.array([row["date"] for row in data])
        sales_series = np.array([float(row["total_sales"]) for row in data])
        profit_series = np.array([float(row["total_profit"]) for row in data])

        self._print_dataset_info(dates, sales_series, profit_series)

        return {"dates": dates, "sales": sales_series, "profits": profit_series}

    def _print_dataset_info(
        self, dates: np.ndarray, sales: np.ndarray, profits: np.ndarray
    ) -> None:
        """Print time series dataset information"""
        print("Dataset Information:")
        print(f"• Time Period:          {dates[0]} to {dates[-1]}")
        print(f"• Number of Data Points: {len(dates)}")
        print(f"• Average Daily Sales:   ${np.mean(sales):.2f}")
        print(f"• Average Daily Profit:  ${np.mean(profits):.2f}")

    def _fit_forecasting_models(self, data: dict) -> dict:
        """Fit Prophet models to the prepared data"""
        print("\nFitting Models...")
        sales_forecaster = ProphetForecaster()
        profit_forecaster = ProphetForecaster()

        sales_forecaster.fit(data["sales"])
        profit_forecaster.fit(data["profits"])
        print("Model fitting completed successfully")
        print("=" * 80)

        return {
            "sales_model": sales_forecaster,
            "profit_model": profit_forecaster,
            "training_data": data,
        }

    def _generate_model_forecasts(self, model_data: dict, forecast_days: int) -> dict:
        """Generate forecasts from both models"""
        return {
            "sales": model_data["sales_model"].predict(forecast_days),
            "profits": model_data["profit_model"].predict(forecast_days),
        }

    def _generate_forecast_dates(self, last_date: datetime, forecast_days: int) -> list:
        """Generate dates for the forecast period"""
        return [last_date + timedelta(days=i + 1) for i in range(forecast_days)]

    def _create_forecast_dataframe(self, dates: list, forecasts: dict) -> DataFrame:
        """Create Spark DataFrame from forecast data"""
        forecast_rows = [
            (date, float(sales), float(profits))
            for date, sales, profits in zip(
                dates, forecasts["sales"], forecasts["profits"]
            )
        ]

        return self.spark.createDataFrame(
            forecast_rows, ["date", "forecasted_sales", "forecasted_profit"]
        )

    def save_to_csv(self, df: DataFrame, output_path: str, filename: str) -> None:
        """
        Save DataFrame to a single CSV file.

        :param df: DataFrame to save
        :param output_path: Base directory path
        :param filename: Name of the CSV file
        """
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Create full path for the output file
        full_path = os.path.join(output_path, filename)
        print(f"Saving to: {full_path}")  # Debugging output

        # Create a temporary directory in the correct output path
        temp_dir = os.path.join(output_path, "_temp")
        print(f"Temporary directory: {temp_dir}")  # Debugging output

        # Save to temporary directory
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Find the generated part file
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        # Move and rename it to the desired output path
        shutil.move(csv_file, full_path)

        # Clean up - remove the temporary directory
        shutil.rmtree(temp_dir)



    '''
HERE'''
  
    def join_transactions_and_products(self, transactions_df) -> None:
        """Join transactions with product data"""
        # Join transactions_df and products_df on product_id
        joined_df = transactions_df.join(
            self.products_df, 
            transactions_df.product_id == self.products_df.product_id,
            how='inner'
        )
        # Select and rename necessary columns to create a "joined_df"
        joined_df = joined_df.select(
            F.col("transaction_id").alias("order_id"),
            F.col("timestamp").alias("order_datetime"),
            "customer_id",
            "quantity",
            transactions_df["product_id"], 
            F.col("sales_price").alias("sales"),
            F.col("cost_to_make").alias("cost")
        )

        # Show joined data for debugging
        joined_df.show(5)
        return joined_df

    def initialize_products(self, products_df):
        self.products_df = products_df

    def process(self, transactions_df):
     update_product = self.products_df.select("product_id", "product_name", "stock")
     update_product.show(5)
     exploded_df = self.explode_transactions(transactions_df)
     transac = self.join_transactions_and_products(exploded_df)
     print(transac.columns)
     # Calculate total amount, number of items per order, round total_amount, format timestamp, and join with customers
     orders_df_sorted = (
        transac.groupBy(
                "order_id", "customer_id", "order_datetime"
            )
            .agg(
                F.count("product_id").alias("num_items"),  # Count number of items
                F.round(F.sum(F.col("quantity") * F.col("sales_price")), 2).alias(
                    "total_amount"
                ),  # Total amount per order and round it
            )
            .select(
                F.col("order_id"),
                F.col("order_datetime"),
                "customer_id",
                F.format_number("total_amount", 2).alias("total_amount"), 
                "num_items",
            )
            .orderBy("order_id")  # Sort by transaction_id (order_id)
         )
     orders_df_sorted.show(5, truncate=False)  # Show first 5 rows of orders

  
    #  order_id|      order_datetime|customer_id|quantity|product_id|sales|cost|
    #  self.process_orders1(transac)

    def process_orders1(self, transactions_df: DataFrame) -> None:
            """Process orders, handle inventory, and generate daily summary"""
            cancelled_items_count = 0
            daily_orders = []
            daily_order_items = []
            product_updates = []
            # Select the specific columns from products_df and rename 'stock' to 'current_stock'
            self.products_update_df = self.products_df.select(
                "product_id", "product_name", "stock"        
            ).withColumnRenamed("stock", "current_stock")  # Rename 'stock' to 'current_stock'

            
            for transaction in transactions_df.collect():  # Loop through each transaction (order)
                order_id = transaction["order_id"]
                order_datetime = transaction["order_datetime"]
                customer_id = transaction["customer_id"]
                product_id = transaction["product_id"]
                product_qty = transaction["quantity"]
                product_price = transaction["sales"]
                    
                # Step 1: Remove items with null quantity
                if product_qty is None or int(product_qty) <= 0:
                    continue  # Skip item

                # Step 2: Check if there's sufficient inventory for the item
                inventory = self.get_product_inventory(product_id)  # Assuming get_product_inventory() fetches current stock for product_id
                product_qty, inventory = float(product_qty), float(inventory)
                if inventory >= product_qty:
                    # Sufficient inventory, proceed with the sale
                    # line_total = product_qty * product_price
                    # daily_order_items.append([order_id, product_id, product_qty, product_price, line_total])
                    self.update_inventory(product_id, product_qty)
                    print("updating")
                    # product_updates.append([product_id, inventory - product_qty])  # Store updated inventory level
                else:
                    # Insufficient inventory, cancel the item
                    cancelled_items_count += 1
                    daily_order_items.append([order_id, product_id, 0, product_price, 0])  # Cancel the item in the order line
                    print(f"⚠️ Item {product_id} (Order {order_id}) cancelled due to insufficient inventory.")
                        
                # Add order-level details for the summary
                # total_order_value = sum(t["quantity"] * t["unit_price"] for t in transaction if t["quantity"] > 0)
                # num_items = len(transaction["items"])
                # daily_orders.append([order_id, order_datetime, customer_id, total_order_value, num_items])
            # daily_order_items.show()
            print("uodated")
            self.products_update_df.show()
            # Create DataFrames for the processed orders and items
            # self.orders_df = self.spark.createDataFrame(daily_orders, ["order_id", "order_datetime", "customer_id", "total_amount", "num_items"])
            # self.order_line_items_df = self.spark.createDataFrame(daily_order_items, ["order_id", "product_id", "quantity", "unit_price", "line_total"])
            
            # Update the product inventory
            # self.products_update_df = self.spark.createDataFrame(product_updates, ["product_id", "updated_inventory"])

            # Generate daily summary
            # self.daily_summary_df = self.generate_daily_summary()

            # Print summary
            # self.print_daily_summary(self.orders_df, self.order_line_items_df, cancelled_items_count)

    def get_product_inventory(self, product_id: int) -> int:
            """Retrieve current inventory for a product"""
            # Assuming current_inventory is a DataFrame with the product inventory details
            inventory = self.products_update_df.filter(col("product_id") == product_id).collect()
            if inventory:
                return inventory[0]["current_stock"]
            return 0

    def update_inventory(self, product_id: int, quantity_sold: int) -> None:
            """Update the inventory after processing an order"""
            # Decrease the stock by the sold quantity
            self.products_update_df = self.products_update_df.withColumn(
                "current_stock", 
                when(col("product_id") == product_id, col("current_stock").cast("double") - quantity_sold).otherwise(col("current_stock").cast("double"))
            )
            # Collect updated data for the specific product_id
            updated_product = self.products_update_df.filter(col("product_id") == product_id).select("product_id", "current_stock").collect()

            # Print the updated current stock for the product_id
            if updated_product:
                print(f"Product ID: {updated_product[0]['product_id']}, New Current Stock: {updated_product[0]['current_stock']}")
