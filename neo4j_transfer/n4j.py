from neo4j import GraphDatabase
from neo4j_transfer.models import Neo4jCredentials
import logging

logger = logging.getLogger("neo4j_transfer")


def validate_credentials(creds: Neo4jCredentials):
    with GraphDatabase.driver(
        creds.uri, auth=(creds.username, creds.password)
    ) as driver:
        driver.verify_connectivity()


def execute_query(creds: Neo4jCredentials, query, params={}, database: str = "neo4j"):
    # Returns a tuple of records, summary, keys
    with GraphDatabase.driver(
        creds.uri, auth=(creds.username, creds.password)
    ) as driver:
        return driver.execute_query(query, params, database=database)
