from neo4j import GraphDatabase
from .models import Neo4jCredentials


def validate_credentials(creds: Neo4jCredentials):
    with GraphDatabase.driver(
        creds.uri, auth=(creds.username, creds.password)
    ) as driver:
        driver.verify_connectivity()


def execute_query(creds: Neo4jCredentials, query, params={}):
    # Returns a tuple of records, summary, keys
    with GraphDatabase.driver(
        creds.uri, auth=(creds.username, creds.password)
    ) as driver:
        return driver.execute_query(query, params, database=creds.database)
