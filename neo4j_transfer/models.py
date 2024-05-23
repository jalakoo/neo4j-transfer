from enum import Enum
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


class KeyTransferSpec(BaseModel):
    """Configuration for transferring Nodes between Neo4j instances


    Args:
        source (str, optional): The property key to use from the source data to use as the Node id. Defaults to "element_id".
        target (str, optional): The property key to use from the target data to use as the Node id. If None, the source key will be copied over to the target node. Defaults to None.
    """

    source: Optional[str] = "element_id"
    target: Optional[str] = "_transfer_element_id"


class TransferSpec(BaseModel):
    """Configuraiton for transferring data between Neo4j instances

    Args:
        node_labels (list[str]): List of Node labels from the source database to transfer.

        relationship_types (list[str], optional): List of Relationship types from the source database to transfer. Types specified will only return if the start and end Nodes are also specified in node_labels arg. Defaults to [].

        should_append_data (bool): Should transferred Nodes and Relationships contain additional properties as specified by this TransferSpec (ie _transfer_element_id, _transfer_timestamp, etc). Default True.

        element_id_key (str): The copy of the source Node/Relationship's element id value will be inserted as the value of this property key to the duplicated Nodes/Relationships. Defaults to "_transfer_element_id".

        timestamp_key (str): The timestamp key identifying when the transfer was initiated will be inserted as the value of this property key to the duplicated Nodes/Relationships. Defaults to "_transfer_timestamp". If set to None, no timestamp to copied data will be added. This will prevent a subsequent undo() call from working.

        timestamp (datetime): Timestamp of transfer. Defaults to datetime when the TransferSpec object created.

    """

    timestamp_key: str = "_transfer_timestamp"
    timestamp: datetime = Field(default_factory=datetime.now)

    node_labels: list[str] | dict[str, KeyTransferSpec] = Field(default_factory=list)
    relationship_types: list[str] | dict[str, KeyTransferSpec] = Field(
        default_factory=list
    )

    def node_mappings(self) -> dict:
        """This function will convert a string list of Node labels to a dictionary of label: KeyTransferSpec. If a dictionary was originally assigned to the node_labels property, this function will return that dictionary unchanged.

        Raises:
            Exception: Raises ValueError if the node_labels property is not a list of strings or a dictionary of type string: KeyTransferSpec.

        Returns:
            dict: Dictionary of Node labels and their corresponding KeyTransferSpec.
        """
        result = {}
        if isinstance(self.node_labels, list):
            for label in self.node_labels:
                if not isinstance(label, str):
                    raise ValueError(f"Node label '{label}' is not of type string")
                # Use default KeyTransferSpec if only Node label and Relationship Types specified
                result[label] = KeyTransferSpec()
        elif isinstance(self.node_labels, dict):
            # Check dict of string: KeyTransferSpec
            for key, value in self.node_labels.items():
                if isinstance(value, KeyTransferSpec):
                    result[key] = value
                else:
                    try:
                        value = KeyTransferSpec(**value)
                        result[key] = value
                    except Exception as e:
                        raise Exception(
                            f"Could not convert value for key '{key}' to a KeyTransferSpec. ERROR: {e}"
                        )
        else:
            raise Exception(
                "Invalid node_labels type. Expect list of strings or dictionary of string: KeyTransferSpec"
            )
        return result

    def relationship_mappings(self) -> dict:
        """Returns a dictionary of Relationship types and their corresponding KeyTransferSpec. If a dictionary was originally assigned to the relationship_types property, this function will return that dictionary unchanged.

        Raises:
            Exception: Raises ValueError if the relationship_types property is not a list of strings or a dictionary of type string: KeyTransferSpec.

        Returns:
            dict: Dictionary of Relationship types and their corresponding KeyTransferSpec.
        """
        result = {}

        if isinstance(self.relationship_types, list):
            # Check is list of strings
            if not all(isinstance(x, str) for x in self.relationship_types):
                raise ValueError("relationship_types must be a list of strings")
            for rel_type in self.relationship_types:
                result[rel_type] = KeyTransferSpec()
        elif isinstance(self.relationship_types, dict):
            # Check dict of string: KeyTransferSpec
            for key, value in self.relationship_types.items():
                if isinstance(value, KeyTransferSpec):
                    result[key] = value
                else:
                    try:
                        value = KeyTransferSpec(**value)
                        result[key] = value
                    except Exception as e:
                        raise Exception(
                            f"Could not convert value for key '{key}' to a KeyTransferSpec. ERROR: {e}"
                        )
        else:
            raise Exception(
                "Invalid relationship_types type. Expect list of strings or dictionary of string: KeyTransferSpec"
            )

        return result

    def __hash__(self):
        return hash((type(self),) + tuple(self.dict().items()))
