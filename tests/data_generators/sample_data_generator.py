import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import os
import uuid
from typing import Dict, List, Tuple

class TestDataGenerator:
    def __init__(self, base_path: str = "/tmp/pipeline_test_data"):
        self.base_path = Path(base_path)
        self.customers = None
        self.transactions = None
        self.products = None
        self.orders = None
        self.quality_metrics = None
        
        # Create base directories
        for layer in ['landing', 'bronze', 'silver', 'gold']:
            for domain in ['finance', 'sales', 'quality']:
                (self.base_path / layer / domain).mkdir(parents=True, exist_ok=True)

    def generate_customer_data(self, num_records: int = 1000) -> pd.DataFrame:
        """Generate customer data with some intentional anomalies"""
        np.random.seed(42)
        
        # Normal data
        data = {
            'customer_id': [f'CUST_{i:06d}' for i in range(num_records)],
            'name': [f'Customer {i}' for i in range(num_records)],
            'email': [f'customer{i}@example.com' for i in range(num_records)],
            'registration_date': [
                (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
                for _ in range(num_records)
            ],
            'credit_score': np.random.normal(700, 50, num_records).astype(int),
            'income': np.random.lognormal(11, 0.5, num_records),  # Log-normal distribution for income
            'region': np.random.choice(['NORTH', 'SOUTH', 'EAST', 'WEST'], num_records),
            'state': np.random.choice(['CA', 'NY', 'TX', 'FL', 'IL'], num_records)
        }
        
        # Introduce anomalies
        anomaly_indices = np.random.choice(num_records, size=int(num_records * 0.05), replace=False)
        data['credit_score'] = pd.Series(data['credit_score']).mask(
            pd.Series(range(num_records)).isin(anomaly_indices),
            np.random.normal(400, 30, len(anomaly_indices))
        )
        
        self.customers = pd.DataFrame(data)
        return self.customers

    def generate_transaction_data(self, num_records: int = 5000) -> pd.DataFrame:
        """Generate transaction data with patterns and anomalies"""
        if self.customers is None:
            self.generate_customer_data()
            
        data = {
            'transaction_id': [str(uuid.uuid4()) for _ in range(num_records)],
            'customer_id': np.random.choice(self.customers['customer_id'], num_records),
            'transaction_date': [
                (datetime.now() - timedelta(days=random.randint(0, 90))).strftime('%Y-%m-%d %H:%M:%S')
                for _ in range(num_records)
            ],
            'amount': np.random.lognormal(5, 1, num_records),  # Log-normal distribution for amounts
            'transaction_type': np.random.choice(['PURCHASE', 'REFUND', 'TRANSFER'], num_records),
            'status': np.random.choice(['SUCCESS', 'FAILED', 'PENDING'], num_records, p=[0.95, 0.03, 0.02])
        }
        
        # Introduce processing time anomalies
        data['processing_time'] = np.random.normal(100, 10, num_records)  # Normal processing time in ms
        anomaly_indices = np.random.choice(num_records, size=int(num_records * 0.02), replace=False)
        data['processing_time'] = pd.Series(data['processing_time']).mask(
            pd.Series(range(num_records)).isin(anomaly_indices),
            np.random.normal(500, 50, len(anomaly_indices))  # Anomalous processing times
        )
        
        self.transactions = pd.DataFrame(data)
        return self.transactions

    def generate_product_data(self, num_records: int = 200) -> pd.DataFrame:
        """Generate product data"""
        categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
        data = {
            'product_id': [f'PROD_{i:06d}' for i in range(num_records)],
            'name': [f'Product {i}' for i in range(num_records)],
            'category': np.random.choice(categories, num_records),
            'price': np.random.uniform(10, 1000, num_records),
            'stock': np.random.randint(0, 1000, num_records)
        }
        
        self.products = pd.DataFrame(data)
        return self.products

    def generate_order_data(self, num_records: int = 3000) -> pd.DataFrame:
        """Generate order data with regional patterns"""
        if self.customers is None:
            self.generate_customer_data()
        if self.products is None:
            self.generate_product_data()
            
        data = {
            'order_id': [f'ORD_{i:08d}' for i in range(num_records)],
            'customer_id': np.random.choice(self.customers['customer_id'], num_records),
            'product_id': np.random.choice(self.products['product_id'], num_records),
            'order_date': [
                (datetime.now() - timedelta(days=random.randint(0, 90))).strftime('%Y-%m-%d')
                for _ in range(num_records)
            ],
            'quantity': np.random.randint(1, 10, num_records),
            'status': np.random.choice(['APPROVED', 'REJECTED', 'PENDING'], num_records, p=[0.85, 0.10, 0.05])
        }
        
        self.orders = pd.DataFrame(data)
        return self.orders

    def generate_quality_metrics(self, num_records: int = 1000) -> pd.DataFrame:
        """Generate quality metrics data with patterns"""
        data = {
            'metric_id': [f'QM_{i:06d}' for i in range(num_records)],
            'metric_date': [
                (datetime.now() - timedelta(days=random.randint(0, 90))).strftime('%Y-%m-%d')
                for _ in range(num_records)
            ],
            'completeness_score': np.random.normal(0.98, 0.02, num_records),
            'accuracy_score': np.random.normal(0.95, 0.03, num_records),
            'timeliness_score': np.random.normal(0.97, 0.02, num_records)
        }
        
        # Introduce quality anomalies
        anomaly_indices = np.random.choice(num_records, size=int(num_records * 0.03), replace=False)
        for score_type in ['completeness_score', 'accuracy_score', 'timeliness_score']:
            data[score_type] = pd.Series(data[score_type]).mask(
                pd.Series(range(num_records)).isin(anomaly_indices),
                np.random.normal(0.70, 0.05, len(anomaly_indices))
            )
        
        self.quality_metrics = pd.DataFrame(data)
        return self.quality_metrics

    def save_data_in_formats(self):
        """Save generated data in multiple formats"""
        datasets = {
            'customers': self.customers,
            'transactions': self.transactions,
            'products': self.products,
            'orders': self.orders,
            'quality_metrics': self.quality_metrics
        }
        
        formats = {
            'csv': lambda df, path: df.to_csv(path, index=False),
            'json': lambda df, path: df.to_json(path, orient='records', lines=True),
            'parquet': lambda df, path: df.to_parquet(path, engine='pyarrow'),
            'delta': lambda df, path: df.to_parquet(path, engine='pyarrow')  # Delta format simulation
        }
        
        for dataset_name, df in datasets.items():
            if df is None:
                continue
                
            for fmt, save_func in formats.items():
                domain = 'finance' if dataset_name in ['customers', 'transactions'] else \
                        'quality' if dataset_name == 'quality_metrics' else 'sales'
                
                path = self.base_path / 'landing' / domain / f'{dataset_name}.{fmt}'
                save_func(df, path)
                print(f"Saved {dataset_name} in {fmt} format at {path}")

    def generate_all_data(self):
        """Generate all test data"""
        self.generate_customer_data()
        self.generate_transaction_data()
        self.generate_product_data()
        self.generate_order_data()
        self.generate_quality_metrics()
        self.save_data_in_formats()

def main():
    generator = TestDataGenerator()
    generator.generate_all_data()
    print("Test data generation complete!")

if __name__ == "__main__":
    main()
