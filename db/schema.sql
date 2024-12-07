-- Pipeline Configuration Tables

CREATE TABLE pipeline_config (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(255) NOT NULL,
    landing_location TEXT NOT NULL,
    target_location TEXT NOT NULL,
    input_format VARCHAR(50) NOT NULL,
    output_format VARCHAR(50) NOT NULL,
    kafka_input_topic VARCHAR(255),
    kafka_output_topic VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE column_encryption_config (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES pipeline_config(id),
    column_name VARCHAR(255) NOT NULL,
    encryption_type VARCHAR(50) NOT NULL
);

CREATE TABLE mapping_version (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES pipeline_config(id),
    version INTEGER NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE column_mapping (
    id SERIAL PRIMARY KEY,
    mapping_version_id INTEGER REFERENCES mapping_version(id),
    source_column VARCHAR(255) NOT NULL,
    target_column VARCHAR(255) NOT NULL,
    transformation_type VARCHAR(50) NOT NULL,
    transformation_rule TEXT,
    source_data_type VARCHAR(50),
    target_data_type VARCHAR(50)
);

CREATE TABLE data_quality_rules (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES pipeline_config(id),
    rule_name VARCHAR(255) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    rule_config JSONB NOT NULL,
    threshold FLOAT NOT NULL,
    enabled BOOLEAN DEFAULT true
);

CREATE TABLE pipeline_runs (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES pipeline_config(id),
    mapping_version_id INTEGER REFERENCES mapping_version(id),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    records_processed INTEGER,
    error_message TEXT
);

CREATE TABLE data_quality_results (
    id SERIAL PRIMARY KEY,
    pipeline_run_id INTEGER REFERENCES pipeline_runs(id),
    rule_id INTEGER REFERENCES data_quality_rules(id),
    pass_count INTEGER,
    fail_count INTEGER,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
