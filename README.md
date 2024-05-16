# Neo4j Transfer
Tool for transferring select data from one active Neo4j instance to another.

## Installing
Using [Pip](https://pip.pypa.io/en/stable/)
```
pip install git+https://github.com/jalakoo/neo4j-transfer.git@main
```


## Usage
```
from neo4j-transfer import {
    Neo4jCredentials,
    TransferSpec,
}

source_creds = Neo4jCredentials(
    uri = "<source_uri>",
    password = "<source_password>"
    username = "<optional_source_username>",
    database = "<optional_source_database>"
)
target_creds = Neo4jCredentials(
    uri = "<target_uri>",
    username = "<target_username>",
    password = "<target_password>"
)
spec = TransferSpec(
    nodes = ["label1", "label2"],
    relationships = ["type1","type2"]
)
transfer(source_creds, target_creds, spec)

````

A transfer can also be undone by passing the same TransferSpec:
```
undo(target_creds, spec)
```