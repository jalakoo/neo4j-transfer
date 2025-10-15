git s# Neo4j Transfer

A Python tool for efficiently transferring selected data from one Neo4j instance to another, with support for batch processing, progress tracking, and transfer metadata.

## Features

- **Selective Data Transfer**: Transfer specific node labels and relationship types
- **Batch Processing**: Efficient large-scale transfers with configurable batch sizes
- **Progress Tracking**: Real-time progress updates during transfers
- **Transfer Metadata**: Optional augmentation with source element IDs and timestamps
- **Undo Capability**: Remove transferred data using the original transfer specification
- **Database Reset**: Option to clear target database before transfer
- **Stoppable Transfers**: Cancel long-running transfers gracefully

## Documentation

📖 **[View Full API Documentation](docs/neo4j_transfer.html)**

## Installation

### From GitHub (Development)
```bash
pip install git+https://github.com/jalakoo/neo4j-transfer.git@main
```

### From PyPI (When Available)
```bash
pip install neo4j-transfer
```

## Quick Start

### Basic Transfer
```python
from neo4j_transfer import Neo4jCredentials, TransferSpec, transfer

# Define source and target database credentials
source_creds = Neo4jCredentials(
    uri="bolt://localhost:7687",
    username="neo4j",
    password="source_password",
    database="neo4j"
)

target_creds = Neo4jCredentials(
    uri="bolt://localhost:7688", 
    username="neo4j",
    password="target_password",
    database="neo4j"
)

# Specify what to transfer
spec = TransferSpec(
    node_labels=["Person", "Company"],
    relationship_types=["WORKS_FOR", "OWNS"]
)

# Execute the transfer
result = transfer(source_creds, target_creds, spec)
print(f"Transfer completed: {result.nodes_created} nodes, {result.relationships_created} relationships")
```

### Transfer with Progress Tracking
```python
from neo4j_transfer import transfer_generator

# Use the generator for real-time progress updates
for update in transfer_generator(source_creds, target_creds, spec):
    if hasattr(update, 'records_completed'):
        progress = update.float_completed()
        print(f"Progress: {progress:.1%}")
```

### Transfer with Metadata Augmentation
```python
spec = TransferSpec(
    node_labels=["Person"],
    relationship_types=["KNOWS"],
    should_append_data=True,  # Add transfer metadata
    element_id_key="_source_element_id",
    timestamp_key="_transfer_timestamp"
)
```

### Undo a Transfer
```python
from neo4j_transfer import undo

# Remove all data transferred with this spec
undo(target_creds, spec)
```

## Advanced Usage

### Database Reset Before Transfer
```python
spec = TransferSpec(
    node_labels=["Person"],
    overwrite_target=True,  # Clear target database first
    batch_size=10000
)
```

### Stoppable Transfer
```python
gen = transfer_generator(source_creds, target_creds, spec)
controller = next(gen)  # Get the controller

# In another thread or after some condition:
controller.request_stop()

# Continue consuming updates
for update in gen:
    # Process updates until stop is requested
    pass
```

## API Reference

### Neo4jCredentials
Credentials for accessing a Neo4j database instance.

- `uri` (str): The URI address of the Neo4j database
- `password` (str): The password for authentication
- `username` (str, optional): Username (default: "neo4j")
- `database` (str, optional): Database name (default: "neo4j")

### TransferSpec
Configuration for data transfer between Neo4j instances.

- `node_labels` (list[str]): Node labels to transfer
- `relationship_types` (list[str], optional): Relationship types to transfer
- `should_append_data` (bool): Add transfer metadata to copied data (default: True)
- `element_id_key` (str): Property key for source element ID (default: "_transfer_element_id")
- `timestamp_key` (str): Property key for transfer timestamp (default: "_transfer_timestamp")
- `overwrite_target` (bool): Clear target database before transfer (default: False)
- `batch_size` (int): Batch size for transfers (default: 10000)

### UploadResult
Result object containing transfer statistics.

- `nodes_created` (int): Number of nodes created
- `relationships_created` (int): Number of relationships created
- `records_total` (int): Total records processed
- `records_completed` (int): Records completed
- `was_successful` (bool): Whether transfer succeeded
- `seconds_to_complete` (float): Transfer duration
- `float_completed()`: Returns completion percentage as float (0.0-1.0)

## Requirements

- Python 3.11+
- Neo4j 4.0+
- Access to both source and target Neo4j instances

## License

MIT License - see [LICENSE.txt](LICENSE.txt) for details.