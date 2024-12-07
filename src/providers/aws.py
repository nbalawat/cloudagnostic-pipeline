from typing import Any, Dict, List
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

from .base import StorageProvider, OrchestratorProvider, MonitoringProvider, EncryptionProvider

class AWSStorageProvider(StorageProvider):
    def __init__(self, config: Dict[str, Any]):
        self.s3 = boto3.client('s3')
        self.bucket = config.get('bucket')
        
    async def read_file(self, path: str) -> bytes:
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=path)
            return response['Body'].read()
        except ClientError as e:
            raise Exception(f"Failed to read file from S3: {str(e)}")
    
    async def write_file(self, path: str, content: bytes) -> bool:
        try:
            self.s3.put_object(Bucket=self.bucket, Key=path, Body=content)
            return True
        except ClientError as e:
            raise Exception(f"Failed to write file to S3: {str(e)}")
    
    async def list_files(self, prefix: str) -> List[str]:
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            return [obj['Key'] for obj in response.get('Contents', [])]
        except ClientError as e:
            raise Exception(f"Failed to list files in S3: {str(e)}")
    
    async def delete_file(self, path: str) -> bool:
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=path)
            return True
        except ClientError as e:
            raise Exception(f"Failed to delete file from S3: {str(e)}")

class AWSOrchestratorProvider(OrchestratorProvider):
    def __init__(self, config: Dict[str, Any]):
        self.batch = boto3.client('batch')
        self.queue = config.get('job_queue')
        self.job_definition = config.get('job_definition')
    
    async def create_job(self, job_config: Dict[str, Any]) -> str:
        try:
            response = self.batch.submit_job(
                jobName=job_config['name'],
                jobQueue=self.queue,
                jobDefinition=self.job_definition,
                containerOverrides={
                    'command': job_config.get('command', []),
                    'environment': [
                        {'name': k, 'value': v}
                        for k, v in job_config.get('environment', {}).items()
                    ]
                }
            )
            return response['jobId']
        except ClientError as e:
            raise Exception(f"Failed to create AWS Batch job: {str(e)}")
    
    async def start_job(self, job_id: str) -> bool:
        # AWS Batch jobs start automatically upon creation
        return True
    
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        try:
            response = self.batch.describe_jobs(jobs=[job_id])
            if not response['jobs']:
                raise Exception(f"Job {job_id} not found")
            job = response['jobs'][0]
            return {
                'status': job['status'],
                'created_at': job['createdAt'],
                'started_at': job.get('startedAt'),
                'stopped_at': job.get('stoppedAt'),
                'exit_code': job.get('container', {}).get('exitCode'),
                'reason': job.get('statusReason')
            }
        except ClientError as e:
            raise Exception(f"Failed to get AWS Batch job status: {str(e)}")
    
    async def cancel_job(self, job_id: str) -> bool:
        try:
            self.batch.terminate_job(jobId=job_id, reason='Cancelled by user')
            return True
        except ClientError as e:
            raise Exception(f"Failed to cancel AWS Batch job: {str(e)}")

class AWSMonitoringProvider(MonitoringProvider):
    def __init__(self, config: Dict[str, Any]):
        self.cloudwatch = boto3.client('cloudwatch')
        self.namespace = config.get('namespace', 'DataPipeline')
    
    async def log_metric(self, metric_name: str, value: float, tags: Dict[str, str]) -> bool:
        try:
            self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=[{
                    'MetricName': metric_name,
                    'Value': value,
                    'Unit': 'None',
                    'Dimensions': [
                        {'Name': k, 'Value': v}
                        for k, v in tags.items()
                    ]
                }]
            )
            return True
        except ClientError as e:
            raise Exception(f"Failed to log metric to CloudWatch: {str(e)}")
    
    async def create_alert(self, alert_config: Dict[str, Any]) -> str:
        try:
            response = self.cloudwatch.put_metric_alarm(
                AlarmName=alert_config['name'],
                MetricName=alert_config['metric_name'],
                Namespace=self.namespace,
                Statistic='Average',
                Period=alert_config.get('period', 300),
                EvaluationPeriods=alert_config.get('evaluation_periods', 1),
                Threshold=alert_config['threshold'],
                ComparisonOperator=alert_config['operator'],
                AlarmActions=alert_config.get('actions', [])
            )
            return alert_config['name']
        except ClientError as e:
            raise Exception(f"Failed to create CloudWatch alarm: {str(e)}")
    
    async def get_metrics(self, metric_name: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        try:
            response = self.cloudwatch.get_metric_data(
                MetricDataQueries=[{
                    'Id': 'm1',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': self.namespace,
                            'MetricName': metric_name
                        },
                        'Period': 300,
                        'Stat': 'Average'
                    }
                }],
                StartTime=start_time,
                EndTime=end_time
            )
            return [
                {'timestamp': ts, 'value': val}
                for ts, val in zip(
                    response['MetricDataResults'][0]['Timestamps'],
                    response['MetricDataResults'][0]['Values']
                )
            ]
        except ClientError as e:
            raise Exception(f"Failed to get CloudWatch metrics: {str(e)}")

class AWSEncryptionProvider(EncryptionProvider):
    def __init__(self, config: Dict[str, Any]):
        self.kms = boto3.client('kms')
        self.key_alias = config.get('key_alias')
    
    async def encrypt_data(self, data: bytes, key_id: str) -> bytes:
        try:
            response = self.kms.encrypt(
                KeyId=key_id,
                Plaintext=data
            )
            return response['CiphertextBlob']
        except ClientError as e:
            raise Exception(f"Failed to encrypt data with KMS: {str(e)}")
    
    async def decrypt_data(self, encrypted_data: bytes, key_id: str) -> bytes:
        try:
            response = self.kms.decrypt(
                KeyId=key_id,
                CiphertextBlob=encrypted_data
            )
            return response['Plaintext']
        except ClientError as e:
            raise Exception(f"Failed to decrypt data with KMS: {str(e)}")
    
    async def rotate_key(self, old_key_id: str) -> str:
        try:
            response = self.kms.create_key(
                Description='Data Pipeline Encryption Key',
                KeyUsage='ENCRYPT_DECRYPT',
                Origin='AWS_KMS'
            )
            new_key_id = response['KeyMetadata']['KeyId']
            
            # Update alias to point to new key
            try:
                self.kms.update_alias(
                    AliasName=self.key_alias,
                    TargetKeyId=new_key_id
                )
            except ClientError:
                self.kms.create_alias(
                    AliasName=self.key_alias,
                    TargetKeyId=new_key_id
                )
            
            return new_key_id
        except ClientError as e:
            raise Exception(f"Failed to rotate KMS key: {str(e)}")
    
    async def get_key_metadata(self, key_id: str) -> Dict[str, Any]:
        try:
            response = self.kms.describe_key(KeyId=key_id)
            return {
                'id': response['KeyMetadata']['KeyId'],
                'arn': response['KeyMetadata']['Arn'],
                'creation_date': response['KeyMetadata']['CreationDate'],
                'status': response['KeyMetadata']['KeyState'],
                'deletion_date': response['KeyMetadata'].get('DeletionDate')
            }
        except ClientError as e:
            raise Exception(f"Failed to get KMS key metadata: {str(e)}")
