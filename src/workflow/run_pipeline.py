import os
import subprocess
import yaml
from dag_generator import DAGGenerator
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_directories():
    """Create necessary directories for the pipeline"""
    dirs = [
        "/data/landing/finance",
        "/data/landing/sales",
        "/data/bronze/finance",
        "/data/bronze/sales",
        "/data/silver/finance",
        "/data/silver/sales",
        "/data/gold/analytics"
    ]
    for dir_path in dirs:
        os.makedirs(dir_path, exist_ok=True)
        logger.info(f"Created directory: {dir_path}")

def generate_sample_data():
    """Generate sample data using the data generator"""
    logger.info("Generating sample data...")
    subprocess.run(["python", "processor/generate_sample_data.py"], check=True)
    logger.info("Sample data generated successfully")

def generate_workflow():
    """Generate Argo Workflow from configuration"""
    logger.info("Generating Argo Workflow...")
    generator = DAGGenerator("config/sample_config.yaml")
    dag = generator.generate_dag()
    workflow = generator.generate_argo_workflow(dag)
    
    # Save workflow
    workflow_path = "workflow/pipeline-workflow.yaml"
    generator.save_workflow(workflow, workflow_path)
    logger.info(f"Workflow saved to: {workflow_path}")
    return workflow_path

def submit_workflow(workflow_path):
    """Submit workflow to Argo"""
    logger.info("Submitting workflow to Argo...")
    subprocess.run(["argo", "submit", workflow_path], check=True)
    logger.info("Workflow submitted successfully")

def main():
    try:
        # Setup directory structure
        setup_directories()
        
        # Generate sample data
        generate_sample_data()
        
        # Generate and submit workflow
        workflow_path = generate_workflow()
        submit_workflow(workflow_path)
        
        logger.info("Pipeline setup and execution completed successfully!")
        
    except Exception as e:
        logger.error(f"Error running pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    main()
