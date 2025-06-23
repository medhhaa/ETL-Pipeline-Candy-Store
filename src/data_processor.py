from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import (
    explode,
    col,
    round as spark_round,
    sum,
    count,
    abs as spark_abs,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
)
from pyspark.sql.window import Window
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
        self.current_inventory = {}
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
        self.daily_orders_list = []
        self.daily_order_items_list = []
        self.daily_summary_list = []

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

    # added function
    def format_values(self, forecast_df):
        forecast_df = forecast_df.select(
            "date",
            F.round("forecasted_sales", 2).alias("forecasted_sales"),
            F.round("forecasted_profit", 2).alias("forecasted_profit"),
        )
        print("Forecasted Profit: ")
        forecast_df.show(5)
        return forecast_df

    # --------------------------------------------------------------------------------------------------------------------
    # Load data:

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

    def load_products(self):
        self.products_df = self.load_mysql_data(
            self.config["mysql_url"],
            self.config["products_table"],
            self.config["mysql_user"],
            self.config["mysql_password"],
        )
        print("Product Data:")
        self.products_df.show(5)
        self.products_df.printSchema()
        print(
            f"Dimensions: {self.products_df.count()} x {len(self.products_df.columns)} \n"
        )

    def load_customers(self):
        self.customers_df = self.load_mysql_data(
            self.config["mysql_url"],
            self.config["customers_table"],
            self.config["mysql_user"],
            self.config["mysql_password"],
        )
        print("Customers Data:")
        self.customers_df.show(5)
        self.customers_df.printSchema()
        print(
            f"Dimensions: {self.customers_df.count()} x {len(self.customers_df.columns)} \n"
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
        return (
            self.spark.read.format("com.mongodb.spark.sql.DefaultSource")
            .option("spark.mongodb.input.uri", mongodb_uri)
            .option("spark.mongodb.input.database", db_name)
            .option("spark.mongodb.input.collection", collection_name)
            .load()
        )

    def load_mongo(self, config, date_range):
        for i, date_str in enumerate(date_range):
            # Construct collection name dynamically
            collection_name = f"{config['mongodb_collection_prefix']}{date_str}"  # print(config["mongodb_uri"], config["mongodb_db"]) # THE DEBUG LINE THAT SAVED MY MONGODB IMPORT:

            # Load the MongoDB collection into a DataFrame
            print(f"Collection - {collection_name}:")
            df = self.load_mongo_data(
                config["mongodb_uri"], config["mongodb_db"], collection_name
            )
            df.show(5)
            df.printSchema()
            # Display Dimensions
            print(f"Dimensions: {df.count()} x {len(df.columns)} \n")
            # Batch Processing
            self.process_daily_transactions(df)

    # ----------------------- Batch Process ------------------------------------------------------------------------------------

    def set_initial_inventory(self) -> None:
        """
        Initialize current_inventory from products_df.
        Convert products_df to a dictionary for easy lookup and update.
        """
        products = self.products_df.collect()
        for row in products:
            self.current_inventory[int(row["product_id"])] = {
                "product_name": row["product_name"],
                "current_stock": int(row["stock"]),
                "unit_price": row["sales_price"],
                "cost_to_make": row["cost_to_make"],
            }

        self.inventory_initialized = True
        print("\nINITIAL INVENTORY SET:")
        for id, inv in self.current_inventory.items():
            print(f"Product ID: {id}, Stock in Inventory: {inv['current_stock']}")

    #  generate orders, order_line, daily_summary
    def generate_orders_line_summary(self, transactions_df: DataFrame) -> None:
        """
        Process transactions for a single day.
        Create orders and order_line_items from the transactions.
        Update inventory based on orders.
        Generate daily summary.
        """
        orders = []
        order_line_items = []

        transactions = transactions_df.collect()

        # Track total sales & profit for each day
        daily_sales = 0.0
        daily_profit = 0.0

        for trans in transactions:
            order_id = trans["transaction_id"]
            customer_id = trans["customer_id"]
            order_datetime = trans["timestamp"]
            items = trans["items"]

            order_total = 0.0
            order_profit = 0.0
            num_item = 0

            flag = 0  # to check if for same order_id, there is any order placed.
            for item in items:
                product_id = item["product_id"]
                quantity = item["qty"]

                if quantity is None:  # Ignore items with null quantity
                    continue

                if (
                    product_id not in self.current_inventory
                ):  # Check if product exists in current_inventory
                    continue

                product_info = self.current_inventory[product_id]
                current_stock = int(product_info["current_stock"])
                unit_price = float(product_info["unit_price"])
                cost_to_make = float(product_info["cost_to_make"])  # for profit
                product_name = product_info["product_name"]

                line_total = 0.0
                line_profit = 0.0

                # Dynamically update the quantity for each row
                if current_stock >= quantity:
                    self.current_inventory[product_id]["current_stock"] -= quantity
                    line_total = unit_price * quantity
                    line_profit = (unit_price - cost_to_make) * quantity
                    num_item += 1
                    flag = 1
                else:
                    print(
                        f"Order for '{product_name}' is cancelled due to insufficient stocks. "
                        f"(ID: {product_id}). Quantity Ordered: {quantity}, Current Stock: {current_stock}."
                    )
                    self.total_cancelled_items += 1
                    quantity = 0

                order_total += line_total
                order_profit += line_profit

                order_line_items.append(
                    {
                        "order_id": order_id,
                        "product_id": product_id,
                        "quantity": quantity,
                        "unit_price": unit_price,
                        "line_total": line_total,
                    }
                )
            # # if no order is placed for this order_id, skip
            # if flag == 0 and num_item == 0:
            #     continue
            orders.append(
                {
                    "order_id": order_id,
                    "order_datetime": order_datetime,
                    "customer_id": customer_id,
                    "total_amount": order_total,
                    "num_items": num_item,
                }
            )

            daily_sales += order_total
            daily_profit += order_profit

        if orders:
            orders_df = self.spark.createDataFrame(orders)
            self.daily_orders_list.append(orders_df)
        if order_line_items:
            order_line_items_df = self.spark.createDataFrame(order_line_items)
            self.daily_order_items_list.append(order_line_items_df)

        daily_summary = {
            "date": datetime.strptime(
                orders[0]["order_datetime"], "%Y-%m-%dT%H:%M:%S.%f"
            ).date(),
            "num_orders": len(orders),
            "total_sales": round(daily_sales, 2),
            "total_profit": round(daily_profit, 2),
        }
        self.daily_summary_list.append(daily_summary)

    def process_daily_transactions(self, transac: DataFrame) -> tuple:
        self.generate_orders_line_summary(transac)

        if self.daily_orders_list:
            combined_orders = self.daily_orders_list[0]
            for df in self.daily_orders_list[1:]:
                combined_orders = combined_orders.union(df)
            self.orders_df = combined_orders

        if self.daily_order_items_list:
            combined_order_items = self.daily_order_items_list[0]
            for df in self.daily_order_items_list[1:]:
                combined_order_items = combined_order_items.union(df)
            self.order_line_items_df = combined_order_items

        if self.daily_summary_list:
            summary_schema = StructType(
                [
                    StructField("date", DateType(), True),
                    StructField("num_orders", IntegerType(), True),
                    StructField("total_sales", DoubleType(), True),
                    StructField("total_profit", DoubleType(), True),
                ]
            )
            self.daily_summary_df = self.spark.createDataFrame(
                self.daily_summary_list, schema=summary_schema
            )
            self.daily_summary_df = self.daily_summary_df.orderBy("date")
            print("\nDaily Summary:")
            self.daily_summary_df.show()

    def sort_orders(self, updated_inventory_df):
        # Calculate total amount, number of items per order, round total_amount, format timestamp, and join with customers
        self.order_line_items_df = self.order_line_items_df.select(
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            F.format_number("line_total", 2).alias("line_total"),
        ).orderBy(  # Select final columns
            "order_id", "product_id"
        )  # Sort by order_id and product_id
        print("Order Line Items:")
        self.order_line_items_df.show(5)

        # ORDERS DF
        self.orders_df = self.orders_df.select(
            "order_id",
            "order_datetime",
            "customer_id",
            F.format_number("total_amount", 2).alias("total_amount"),
            "num_items",
        ).orderBy("order_id")
        print("Orders: ")
        self.orders_df.show(5)

        # PRODUCTS UPDATED DF
        updated_inventory_df = updated_inventory_df.select(
            "product_id",
            "product_name",
            "current_stock",
        )
        print("Products Updated: ")
        updated_inventory_df.show(5)
        return updated_inventory_df

    def update_inventory_table(self) -> DataFrame:
        """
        Create a Spark DataFrame from the updated inventory.
        This represents the products_updated table.
        """
        updated_inventory = []
        for p_id, inv in self.current_inventory.items():
            updated_inventory.append(
                {
                    "product_id": p_id,
                    "product_name": inv["product_name"],
                    "current_stock": inv["current_stock"],
                }
            )
        inventory_df = self.spark.createDataFrame(updated_inventory)
        return inventory_df.orderBy("product_id")

    # --------------------------------------------------------SAVE TO CSV----------------------------------------------------------------

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

        # Save to temporary directory
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Find the generated part file
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        # Move and rename it to the desired output path
        shutil.move(csv_file, full_path)

        # Clean up - remove the temporary directory
        shutil.rmtree(temp_dir)

    def save_outputs(self, config, updated_inventory_df):
        self.save_to_csv(self.orders_df, config["output_path"], "orders.csv")
        self.save_to_csv(
            self.order_line_items_df,
            config["output_path"],
            "order_line_items.csv",
        )
        self.save_to_csv(
            self.daily_summary_df, config["output_path"], "daily_summary.csv"
        )
        self.save_to_csv(
            updated_inventory_df, config["output_path"], "products_updated.csv"
        )
