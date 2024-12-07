from typing import Dict, List
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import networkx as nx

from src.workflow.dag_generator import DAGGenerator, Task
from src.quality.rule_engine import QualityChecker
from src.processor.data_processor import DataProcessor
from src.messaging.event_publisher import EventPublisher
from src.db.models import PipelineRun, TaskRun

logger = logging.getLogger(__name__)

class PipelineExecutor:
    def __init__(self, mapping_file: str, config: Dict):
        self.dag_generator = DAGGenerator(mapping_file)
        self.config = config
        self.data_processor = DataProcessor(config)
        self.quality_checker = QualityChecker(config)
        self.event_publisher = EventPublisher(config)
        self.max_parallel_tasks = config.get('max_parallel_tasks', 3)
        
    def execute_task(self, task: Task, pipeline_run_id: str) -> bool:
        """Execute a single task and return success status."""
        try:
            logger.info(f"Executing task: {task.id}")
            
            # Create task run record
            task_run = TaskRun(
                pipeline_run_id=pipeline_run_id,
                task_id=task.id,
                status='RUNNING',
                start_time=datetime.utcnow()
            )
            
            # Execute based on task type
            if task.type == 'load':
                success = self.data_processor.load_data(
                    source_config=task.config['source'],
                    target_config=task.config['target'],
                    layer=task.layer,
                    table=task.table
                )
            elif task.type == 'quality_check':
                success = self.quality_checker.run_checks(
                    checks=task.config,
                    layer=task.layer,
                    table=task.table
                )
            elif task.type == 'transform':
                success = self.data_processor.transform_data(
                    transformation_config=task.config,
                    layer=task.layer,
                    table=task.table
                )
            else:
                raise ValueError(f"Unknown task type: {task.type}")
            
            # Update task status
            task_run.status = 'SUCCESS' if success else 'FAILED'
            task_run.end_time = datetime.utcnow()
            
            # Publish task completion event
            self.event_publisher.publish_task_event(
                task_id=task.id,
                status=task_run.status,
                details={
                    'layer': task.layer,
                    'table': task.table,
                    'type': task.type
                }
            )
            
            return success
            
        except Exception as e:
            logger.error(f"Error executing task {task.id}: {str(e)}")
            task_run.status = 'FAILED'
            task_run.error_message = str(e)
            task_run.end_time = datetime.utcnow()
            
            # Publish error event
            self.event_publisher.publish_error_event(
                task_id=task.id,
                error=str(e),
                details={
                    'layer': task.layer,
                    'table': task.table,
                    'type': task.type
                }
            )
            return False
    
    def get_ready_tasks(self, dag: nx.DiGraph, completed_tasks: Set[str]) -> List[Task]:
        """Get tasks whose dependencies are all completed."""
        ready_tasks = []
        for node in dag.nodes():
            if node not in completed_tasks:
                dependencies = list(dag.predecessors(node))
                if all(dep in completed_tasks for dep in dependencies):
                    task = dag.nodes[node]['task']
                    ready_tasks.append(task)
        return ready_tasks
    
    def execute_pipeline(self) -> bool:
        """Execute the complete pipeline DAG."""
        try:
            # Generate DAG
            dag = self.dag_generator.generate_dag()
            
            # Create pipeline run record
            pipeline_run = PipelineRun(
                status='RUNNING',
                start_time=datetime.utcnow()
            )
            
            # Initialize tracking sets
            completed_tasks = set()
            failed_tasks = set()
            
            # Execute tasks in parallel with dependency management
            with ThreadPoolExecutor(max_workers=self.max_parallel_tasks) as executor:
                while len(completed_tasks) < len(dag.nodes()):
                    # Get tasks ready for execution
                    ready_tasks = self.get_ready_tasks(dag, completed_tasks)
                    if not ready_tasks:
                        break
                    
                    # Submit ready tasks
                    future_to_task = {
                        executor.submit(self.execute_task, task, pipeline_run.id): task 
                        for task in ready_tasks
                    }
                    
                    # Process completed tasks
                    for future in as_completed(future_to_task):
                        task = future_to_task[future]
                        try:
                            success = future.result()
                            if success:
                                completed_tasks.add(task.id)
                            else:
                                failed_tasks.add(task.id)
                                # Stop pipeline if task fails
                                if not self.config.get('continue_on_failure', False):
                                    raise ValueError(f"Task {task.id} failed")
                        except Exception as e:
                            logger.error(f"Task {task.id} failed with error: {str(e)}")
                            failed_tasks.add(task.id)
                            if not self.config.get('continue_on_failure', False):
                                raise
            
            # Update pipeline status
            pipeline_run.end_time = datetime.utcnow()
            pipeline_run.status = 'SUCCESS' if not failed_tasks else 'FAILED'
            
            # Publish pipeline completion event
            self.event_publisher.publish_pipeline_event(
                pipeline_run_id=pipeline_run.id,
                status=pipeline_run.status,
                details={
                    'completed_tasks': list(completed_tasks),
                    'failed_tasks': list(failed_tasks)
                }
            )
            
            return not failed_tasks
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            if pipeline_run:
                pipeline_run.status = 'FAILED'
                pipeline_run.end_time = datetime.utcnow()
                pipeline_run.error_message = str(e)
            
            # Publish pipeline failure event
            self.event_publisher.publish_pipeline_event(
                pipeline_run_id=pipeline_run.id,
                status='FAILED',
                details={'error': str(e)}
            )
            return False
        
    def visualize_pipeline(self, output_file: str):
        """Generate a visualization of the pipeline DAG."""
        self.dag_generator.visualize_dag(output_file)
