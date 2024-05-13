# Neo4j Transfer
Tool for transferring select data from one active Neo4j instance to another.


## Requirements
Poetry

## Usage

```
import neo4j-transfer as nt

source_creds = Neo4jCredentials()
target_creds = Neo4jCredentials()
spec = nt.TransferSpec(
    nodes = [],
    relationships = []
)
nt.transfer(source_creds, target_creds, spec)

````