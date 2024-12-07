from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import random
import uuid

def create_spark_session():
    return SparkSession.builder \
        .appName("SampleDataGenerator") \
        .getOrCreate()

def generate_customer_data(spark, num_records=1000):
    data = []
    for i in range(num_records):
        customer_id = f"CUST_{i:06d}"
        data.append({
            "customer_id": customer_id,
            "name": f"Customer {i}",
            "email": f"customer{i}@example.com",
            "registration_date": (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat()
        })
    return spark.createDataFrame(data)

def generate_account_data(spark, num_records=2000):
    account_types = ["SAVINGS", "CHECKING", "CREDIT", "INVESTMENT"]
    data = []
    for i in range(num_records):
        customer_id = f"CUST_{random.randint(0, 999):06d}"
        data.append({
            "account_id": f"ACC_{i:08d}",
            "customer_id": customer_id,
            "account_type": random.choice(account_types),
            "balance": round(random.uniform(1000, 100000), 2),
            "opening_date": (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat()
        })
    return spark.createDataFrame(data)

def generate_transaction_data(spark, num_records=5000):
    transaction_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT"]
    data = []
    for i in range(num_records):
        data.append({
            "transaction_id": str(uuid.uuid4()),
            "account_id": f"ACC_{random.randint(0, 1999):08d}",
            "transaction_type": random.choice(transaction_types),
            "amount": round(random.uniform(10, 5000), 2),
            "transaction_date": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat()
        })
    return spark.createDataFrame(data)

def generate_product_data(spark, num_records=100):
    categories = ["ELECTRONICS", "CLOTHING", "BOOKS", "HOME", "SPORTS"]
    data = []
    for i in range(num_records):
        data.append({
            "product_id": f"PROD_{i:04d}",
            "product_name": f"Product {i}",
            "category": random.choice(categories),
            "price": round(random.uniform(10, 1000), 2),
            "stock_quantity": random.randint(0, 1000)
        })
    return spark.createDataFrame(data)

def generate_order_data(spark, num_records=3000):
    data = []
    for i in range(num_records):
        customer_id = f"CUST_{random.randint(0, 999):06d}"
        data.append({
            "order_id": f"ORD_{i:06d}",
            "customer_id": customer_id,
            "product_id": f"PROD_{random.randint(0, 99):04d}",
            "quantity": random.randint(1, 10),
            "order_date": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat()
        })
    return spark.createDataFrame(data)

def main():
    spark = create_spark_session()
    
    # Generate Finance data
    customers_df = generate_customer_data(spark)
    accounts_df = generate_account_data(spark)
    transactions_df = generate_transaction_data(spark)
    
    # Generate Sales data
    products_df = generate_product_data(spark)
    orders_df = generate_order_data(spark)
    
    # Write Finance data
    customers_df.write.mode("overwrite").csv("/data/landing/finance/customers")
    accounts_df.write.mode("overwrite").json("/data/landing/finance/accounts")
    transactions_df.write.mode("overwrite").parquet("/data/landing/finance/transactions")
    
    # Write Sales data
    products_df.write.mode("overwrite").json("/data/landing/sales/products")
    orders_df.write.mode("overwrite").parquet("/data/landing/sales/orders")
    
    print("Sample data generated successfully!")

if __name__ == "__main__":
    main()
