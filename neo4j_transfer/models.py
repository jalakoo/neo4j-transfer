from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import os
import binascii


class Neo4jCredentials(BaseModel):
    """Credentials for accessing a Neo4j database instance

    Args:
        uri (str): The URI address of the Neo4j database.
        password (str): The password for authentication with the Neo4j database.
        username (str): The username for authentication with the Neo4j database. Default is "neo4j".
        database (str): The database to use for multi-database instances. Defaults to "neo4j".

    Returns:
        _type_: _description_
    """

    uri: str
    password: str
    username: Optional[str] = "neo4j"
    database: Optional[str] = "neo4j"

    def __hash__(self):
        return hash((type(self),) + tuple(self.dict().items()))

    def __xgetstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state


def generate_id():
    return binascii.hexlify(os.urandom(8)).decode()


class TransferSpec(BaseModel):
    """Configuraiton for transferring data between Neo4j instances

    Args:
        node_labels (list[str]): List of Node labels from the source database to transfer.

        relationship_types (list[str], optional): List of Relationship types from the source database to transfer. Types specified will only return if the start and end Nodes are also specified in node_labels arg. Defaults to [].

        should_append_data (bool): Should transferred Nodes and Relationships contain additional properties as specified by this TransferSpec (ie _transfer_element_id, _transfer_timestamp, etc). Default True.

        element_id_key (str): The copy of the source Node/Relationship's element id value will be inserted as the value of this property key to the duplicated Nodes/Relationships. Defaults to "_transfer_element_id".

        timestamp_key (str): The timestamp key identifying when the transfer was initiated will be inserted as the value of this property key to the duplicated Nodes/Relationships. Defaults to "_transfer_timestamp".

        timestamp (datetime): Timestamp of transfer. Defaults to datetime when the TransferSpec object created.

    """

    node_labels: list[str]
    relationship_types: Optional[list[str]] = []
    should_append_data: bool = True
    element_id_key: str = "_transfer_element_id"
    timestamp_key: str = "_transfer_timestamp"
    timestamp: datetime = Field(default_factory=datetime.now)

    def __hash__(self):
        return hash((type(self),) + tuple(self.dict().items()))
