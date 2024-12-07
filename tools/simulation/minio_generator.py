#!/usr/bin/env python3
import os
import json
import random
import argparse
import datetime
import pandas as pd
from pathlib import Path
from typing import Dict, List
from minio import Minio
from io import BytesIO

class MinioDataGenerator:
    def __init__(
        self,
        minio_endpoint: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        secure: bool = False
    ):
        self.minio_client = Minio(
            minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.bucket_name = bucket_name
        
        # Ensure bucket exists
        if not self.minio_client.bucket_exists(bucket_name):
            self.minio_client.make_bucket(bucket_name)

    def generate_transaction_data(self, num_records: int) -> pd.DataFrame:
        """Generate synthetic transaction data."""
        transactions = []
        for _ in range(num_records):
            timestamp = datetime.datetime.now() - datetime.timedelta(
                minutes=random.randint(0, 60)
            )
            transaction = {
                'transaction_id': f'TX{random.randint(10000, 99999)}',
                'account_id': f'ACC{random.randint(1000, 9999)}',
                'customer_id': f'CUS{random.randint(1000, 9999)}',
                'transaction_type': random.choice(['DEBIT', 'CREDIT', 'TRANSFER']),
                'amount': round(random.uniform(10, 10000), 2),
                'currency': 'USD',
                'timestamp': timestamp.isoformat(),
                'status': random.choice(['COMPLETED', 'PENDING', 'FAILED']),
                'branch_id': f'BR{random.randint(100, 999)}'
            }
            transactions.append(transaction)
        return pd.DataFrame(transactions)

    def generate_customer_data(self, num_records: int) -> pd.DataFrame:
        """Generate synthetic customer data."""
        customers = []
        for _ in range(num_records):
            customer = {
                'customer_id': f'CUS{random.randint(1000, 9999)}',
                'first_name': f'FirstName{random.randint(1, 100)}',
                'last_name': f'LastName{random.randint(1, 100)}',
                'email': f'customer{random.randint(1, 1000)}@example.com',
                'phone_number': f'+1{random.randint(1000000000, 9999999999)}',
                'risk_score': random.randint(1, 100),
                'customer_type': random.choice(['INDIVIDUAL', 'BUSINESS']),
                'status': random.choice(['ACTIVE', 'INACTIVE', 'SUSPENDED'])
            }
            customers.append(customer)
        return pd.DataFrame(customers)

    def upload_to_minio(
        self,
        data: pd.DataFrame,
        file_type: str,
        timestamp: datetime.datetime
    ) -> str:
        """Upload data to MinIO and return the object path."""
        timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
        
        # Create CSV in memory
        csv_buffer = BytesIO()
        data.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        file_size = csv_buffer.getbuffer().nbytes
        
        # Define object path
        object_path = f"{file_type}/{timestamp_str}/{file_type}_{timestamp_str}.csv"
        
        # Upload CSV
        self.minio_client.put_object(
            self.bucket_name,
            object_path,
            csv_buffer,
            file_size,
            content_type='text/csv'
        )
        
        # Generate and upload metadata
        metadata = self.generate_metadata(object_path, data, file_size)
        metadata_buffer = BytesIO(json.dumps(metadata, indent=2).encode())
        metadata_size = metadata_buffer.getbuffer().nbytes
        metadata_path = f"{object_path}.metadata.json"
        
        self.minio_client.put_object(
            self.bucket_name,
            metadata_path,
            metadata_buffer,
            metadata_size,
            content_type='application/json'
        )
        
        return object_path

    def generate_metadata(
        self,
        object_path: str,
        data: pd.DataFrame,
        file_size: int
    ) -> Dict:
        """Generate metadata for the file."""
        return {
            'object_path': object_path,
            'bucket': self.bucket_name,
            'record_count': len(data),
            'file_size': file_size,
            'creation_time': datetime.datetime.now().isoformat(),
            'columns': list(data.columns),
            'data_types': {col: str(dtype) for col, dtype in data.dtypes.items()}
        }

def main():
    parser = argparse.ArgumentParser(description='Generate synthetic data and upload to MinIO')
    parser.add_argument('--endpoint', required=True, help='MinIO endpoint')
    parser.add_argument('--access-key', required=True, help='MinIO access key')
    parser.add_argument('--secret-key', required=True, help='MinIO secret key')
    parser.add_argument('--bucket', required=True, help='MinIO bucket name')
    parser.add_argument('--file-type', choices=['transactions', 'customers'], required=True)
    parser.add_argument('--num-records', type=int, default=1000)
    parser.add_argument('--secure', action='store_true', help='Use HTTPS')
    args = parser.parse_args()

    generator = MinioDataGenerator(
        args.endpoint,
        args.access_key,
        args.secret_key,
        args.bucket,
        args.secure
    )
    
    if args.file_type == 'transactions':
        data = generator.generate_transaction_data(args.num_records)
    else:
        data = generator.generate_customer_data(args.num_records)
    
    timestamp = datetime.datetime.now()
    object_path = generator.upload_to_minio(data, args.file_type, timestamp)
    
    print(f"Generated and uploaded: s3://{args.bucket}/{object_path}")
    print(f"Metadata: s3://{args.bucket}/{object_path}.metadata.json")

if __name__ == '__main__':
    main()
