from typing import Dict, List, Set
import networkx as nx
from dataclasses import dataclass
from sqlalchemy.orm import Session
from sqlalchemy import and_

from src.db.models import TableMapping, TableDependency, QualityRuleMapping

@dataclass
class Task:
    id: str
    type: str  # 'load', 'quality_check', 'transform'
    layer: str  # 'bronze', 'silver', 'gold'
    table: str
    config: Dict
    dependencies: List[str] = None

class DAGGenerator:
    def __init__(self, db_session: Session):
        self.db = db_session
        self.dag = nx.DiGraph()
        self.tasks: Dict[str, Task] = {}
    
    def generate_task_id(self, task_type: str, layer: str, table: str) -> str:
        """Generate unique task ID."""
        return f"{task_type}_{layer}_{table}"
    
    def add_task(self, task: Task):
        """Add task to DAG with dependencies."""
        self.tasks[task.id] = task
        self.dag.add_node(task.id, task=task)
        if task.dependencies:
            for dep in task.dependencies:
                self.dag.add_edge(dep, task.id)
    
    def create_load_task(self, mapping: TableMapping) -> Task:
        """Create data loading task."""
        task_id = self.generate_task_id("load", mapping.layer, mapping.name)
        config = {
            'source': {
                'type': mapping.source_type,
                'path': mapping.source_path
            },
            'target': {
                'format': mapping.target_format,
                'path': mapping.target_path
            }
        }
        if mapping.config:
            config.update(mapping.config)
            
        return Task(
            id=task_id,
            type="load",
            layer=mapping.layer,
            table=mapping.name,
            config=config
        )
    
    def create_quality_task(self, mapping: TableMapping, load_task_id: str) -> Task:
        """Create quality check task."""
        task_id = self.generate_task_id("quality", mapping.layer, mapping.name)
        
        # Get quality rules for this table
        quality_rules = []
        rule_mappings = self.db.query(QualityRuleMapping).filter(
            and_(
                QualityRuleMapping.table_mapping_id == mapping.id,
                QualityRuleMapping.enabled == True
            )
        ).all()
        
        for rule_mapping in rule_mappings:
            rule_config = {
                'rule_type': rule_mapping.rule.rule_type,
                'parameters': rule_mapping.parameters
            }
            quality_rules.append(rule_config)
        
        return Task(
            id=task_id,
            type="quality_check",
            layer=mapping.layer,
            table=mapping.name,
            config={'rules': quality_rules},
            dependencies=[load_task_id]
        )
    
    def create_transform_task(self, mapping: TableMapping, dependencies: List[str]) -> Task:
        """Create transformation task."""
        task_id = self.generate_task_id("transform", mapping.layer, mapping.name)
        config = {
            'transformation_sql': mapping.transformation_sql,
            'target': {
                'format': mapping.target_format,
                'path': mapping.target_path
            }
        }
        if mapping.config:
            config.update(mapping.config)
            
        return Task(
            id=task_id,
            type="transform",
            layer=mapping.layer,
            table=mapping.name,
            config=config,
            dependencies=dependencies
        )
    
    def process_table_mapping(self, mapping: TableMapping):
        """Process a single table mapping."""
        # Get dependencies
        dependencies = self.db.query(TableDependency).filter(
            TableDependency.table_mapping_id == mapping.id
        ).all()
        
        if not dependencies and mapping.source_type != 'table':
            # This is a source table (bronze layer)
            load_task = self.create_load_task(mapping)
            self.add_task(load_task)
            
            # Add quality check if there are rules
            quality_rules = self.db.query(QualityRuleMapping).filter(
                QualityRuleMapping.table_mapping_id == mapping.id
            ).first()
            if quality_rules:
                quality_task = self.create_quality_task(mapping, load_task.id)
                self.add_task(quality_task)
        else:
            # This is a transformation (silver/gold layer)
            dep_tasks = []
            for dep in dependencies:
                dep_mapping = dep.dependency_table
                dep_quality_task = self.generate_task_id(
                    "quality", dep_mapping.layer, dep_mapping.name
                )
                dep_tasks.append(dep_quality_task)
            
            transform_task = self.create_transform_task(mapping, dep_tasks)
            self.add_task(transform_task)
            
            # Add quality check if there are rules
            quality_rules = self.db.query(QualityRuleMapping).filter(
                QualityRuleMapping.table_mapping_id == mapping.id
            ).first()
            if quality_rules:
                quality_task = self.create_quality_task(mapping, transform_task.id)
                self.add_task(quality_task)
    
    def generate_dag(self) -> nx.DiGraph:
        """Generate the complete DAG from database mappings."""
        # Get all enabled table mappings
        mappings = self.db.query(TableMapping).filter(
            TableMapping.enabled == True
        ).order_by(
            TableMapping.layer  # Process in layer order (bronze -> silver -> gold)
        ).all()
        
        # Process each mapping
        for mapping in mappings:
            self.process_table_mapping(mapping)
        
        # Validate DAG is acyclic
        if not nx.is_directed_acyclic_graph(self.dag):
            raise ValueError("Generated graph contains cycles")
        
        return self.dag
    
    def get_execution_order(self) -> List[Task]:
        """Get tasks in topological order for execution."""
        return [self.tasks[task_id] for task_id in nx.topological_sort(self.dag)]
    
    def visualize_dag(self, output_file: str):
        """Generate a visualization of the DAG."""
        try:
            import graphviz
            dot = graphviz.Digraph(comment='Pipeline DAG')
            
            # Add nodes
            for task_id, task in self.tasks.items():
                label = f"{task.type}\\n{task.layer}.{task.table}"
                dot.node(task_id, label)
            
            # Add edges
            for edge in self.dag.edges():
                dot.edge(edge[0], edge[1])
            
            # Save visualization
            dot.render(output_file, format='png', cleanup=True)
        except ImportError:
            print("graphviz package not installed. Skip visualization.")
