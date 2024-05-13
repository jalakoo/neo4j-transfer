from pydantic import BaseModel
from typing import Optional


class Neo4jCredentials(BaseModel):
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


class TransferSpec(BaseModel):
    node_labels: list[str]
    relationship_types: Optional[list[str]] = []
    override_keys: Optional[dict] = {}
    transfer_key: str = (
        "_element_id"  # Required to work around needing to specify a key for every node
    )

    def __hash__(self):
        return hash((type(self),) + tuple(self.dict().items()))


class TransferResult(BaseModel):
    summaries: list
