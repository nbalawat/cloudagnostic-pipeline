#!/usr/bin/env python3
import os
import json
import random
import argparse
import datetime
import pandas as pd
from pathlib import Path
from typing import Dict, List

class DataFileGenerator:
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
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

    def save_file(self, data: pd.DataFrame, file_type: str, timestamp: datetime.datetime):
        """Save data to file with appropriate naming convention."""
        timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
        
        if file_type == 'transactions':
            filename = f'transactions_{timestamp_str}.csv'
            filepath = self.output_dir / 'transactions' / filename
        elif file_type == 'customers':
            filename = f'customers_{timestamp_str}.csv'
            filepath = self.output_dir / 'customers' / filename
        else:
            raise ValueError(f"Unknown file type: {file_type}")

        filepath.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(filepath, index=False)
        return filepath

    def generate_metadata(self, filepath: Path, data: pd.DataFrame) -> Dict:
        """Generate metadata for the file."""
        return {
            'filename': filepath.name,
            'record_count': len(data),
            'file_size': os.path.getsize(filepath),
            'creation_time': datetime.datetime.now().isoformat(),
            'columns': list(data.columns),
            'data_types': {col: str(dtype) for col, dtype in data.dtypes.items()}
        }

    def save_metadata(self, metadata: Dict, filepath: Path):
        """Save metadata to a JSON file."""
        metadata_path = filepath.with_suffix('.metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

def main():
    parser = argparse.ArgumentParser(description='Generate synthetic data files')
    parser.add_argument('--output-dir', required=True, help='Output directory for files')
    parser.add_argument('--file-type', choices=['transactions', 'customers'], required=True)
    parser.add_argument('--num-records', type=int, default=1000)
    args = parser.parse_args()

    generator = DataFileGenerator(args.output_dir)
    
    if args.file_type == 'transactions':
        data = generator.generate_transaction_data(args.num_records)
    else:
        data = generator.generate_customer_data(args.num_records)
    
    timestamp = datetime.datetime.now()
    filepath = generator.save_file(data, args.file_type, timestamp)
    metadata = generator.generate_metadata(filepath, data)
    generator.save_metadata(metadata, filepath)
    
    print(f"Generated file: {filepath}")
    print(f"Generated metadata: {filepath.with_suffix('.metadata.json')}")

if __name__ == '__main__':
    main()
