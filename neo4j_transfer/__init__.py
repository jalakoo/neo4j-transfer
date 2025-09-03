from neo4j_transfer.models import Neo4jCredentials, TransferSpec, UploadResult
from neo4j_transfer.n4j import validate_credentials, execute_query
from neo4j_transfer._logger import logger
from neo4j import GraphDatabase, Driver, Session
from typing import List, Dict, Any, Generator
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

DEFAULT_BATCH_SIZE = 50000

###############################################################################
# Private Helper Functions
###############################################################################

def _safe_label(name: str) -> str:
    """Ensure the supplied label/rel-type is a bare identifier."""
    if name.isidentifier():
        return f"`{name}`"
    raise ValueError(f"Illegal label/type: {name!r}")

def _batch_create_nodes(
    tgt_sess: Session,
    rows: List[dict[str, Any]],
    id_map: Dict[str, str],
) -> None:
    """Insert a batch of nodes in one round-trip."""
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        lbl_key = tuple(sorted(r["labels"]))
        groups[lbl_key].append(r)

    for labels, batch in groups.items():
        label_fragment = ":" + ":".join(_safe_label(l) for l in labels) if labels else ""
        cypher = f"""
        UNWIND $batch AS row
        CREATE (n{label_fragment})
        SET n = row.props
        RETURN elementId(n) AS new_id, row.eid AS old_id
        """
        for rec in tgt_sess.run(cypher, batch=batch):
            id_map[rec["old_id"]] = rec["new_id"]

def _batch_create_relationships(
    tgt_sess: Session,
    rows: List[dict[str, Any]],
) -> List[Dict]:
    """Insert a batch of relationships in one round-trip."""
    relationships: List[Dict] = []
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        groups[r["type"]].append(r)

    for rel_type, batch in groups.items():
        cypher = f"""
        UNWIND $batch AS row
        MATCH (a) WHERE elementId(a) = row.start
        MATCH (b) WHERE elementId(b) = row.end
        CREATE (a)-[r:{_safe_label(rel_type)}]->(b)
        SET r = row.props
        RETURN elementId(r) AS rel_id, type(r) AS rel_type, elementId(a) AS start_id, elementId(b) AS end_id
        """
        for rec in tgt_sess.run(cypher, batch=batch):
            relationships.append({
                "rel_id": rec["rel_id"],
                "type": rec["rel_type"],
                "start_node": rec["start_id"],
                "end_node": rec["end_id"]
            })
    
    return relationships

def _copy_nodes(
    src: Session, 
    tgt: Session, 
    labels: List[str], 
    page_size: int = DEFAULT_BATCH_SIZE
) -> Dict[str, str]:
    """Copy nodes with specified labels."""
    id_map: Dict[str, str] = {}
    skip = 0

    while True:
        if labels:
            result = src.run(
                """
                MATCH (n)
                WHERE any(label IN labels(n) WHERE label IN $labels)
                WITH n SKIP $skip LIMIT $limit
                RETURN elementId(n) AS eid, labels(n) AS labels, properties(n) AS props
                """,
                skip=skip, limit=page_size, labels=labels
            )
        else:
            result = src.run(
                """
                MATCH (n)
                WITH n SKIP $skip LIMIT $limit
                RETURN elementId(n) AS eid, labels(n) AS labels, properties(n) AS props
                """,
                skip=skip, limit=page_size
            )
        
        batch = [dict(record) for record in result]
        if not batch:
            break
        
        _batch_create_nodes(tgt, batch, id_map)
        skip += page_size
        logger.info(f"    … {skip} nodes processed")

    return id_map

def _copy_relationships(
    src: Session,
    tgt: Session,
    id_map: Dict[str, str],
    types: List[str] = None,
    page_size: int = DEFAULT_BATCH_SIZE
) -> List[Dict]:
    """Copy relationships with optional type filtering."""
    all_relationships: List[Dict] = []
    skip = 0

    while True:
        if types:
            result = src.run(
                """
                MATCH (a)-[r]->(b)
                WHERE type(r) IN $types
                WITH r, a, b SKIP $skip LIMIT $limit
                RETURN type(r) AS type, elementId(a) AS start, elementId(b) AS end, properties(r) AS props
                """,
                skip=skip, limit=page_size, types=types
            )
        else:
            result = src.run(
                """
                MATCH (a)-[r]->(b)
                WITH r, a, b SKIP $skip LIMIT $limit
                RETURN type(r) AS type, elementId(a) AS start, elementId(b) AS end, properties(r) AS props
                """,
                skip=skip, limit=page_size
            )

        batch = []
        for rec in result:
            start_tgt = id_map.get(rec["start"])
            end_tgt = id_map.get(rec["end"])
            if start_tgt and end_tgt:
                batch.append({
                    "type": rec["type"],
                    "start": start_tgt,
                    "end": end_tgt,
                    "props": rec["props"],
                })

        if not batch:
            break

        batch_relationships = _batch_create_relationships(tgt, batch)
        all_relationships.extend(batch_relationships)
        skip += page_size
        logger.info(f"    … {skip} relationships processed")

    return all_relationships

def _reset_target_db(creds: Neo4jCredentials, batch_size: int = DEFAULT_BATCH_SIZE) -> None:
    """Reset target database by dropping constraints, indexes, and all data."""
    logger.info("=== Resetting Target DB ===")

    driver: Driver = GraphDatabase.driver(creds.uri, auth=(creds.username, creds.password))
    try:
        with driver.session(database=creds.database) as session:
            # Drop constraints
            result = session.run("SHOW CONSTRAINTS")
            for record in result:
                constraint_name = record.get("name")
                if constraint_name:
                    session.run(f"DROP CONSTRAINT {constraint_name}")

            # Drop indexes
            result = session.run("SHOW INDEXES")
            for record in result:
                index_name = record.get("name")
                if index_name:
                    session.run(f"DROP INDEX {index_name} IF EXISTS")

            # Delete all nodes and relationships in batches
            deleted_count = -1
            while deleted_count != 0:
                result = session.run("""
                    MATCH (n)
                    OPTIONAL MATCH (n)-[r]-()
                    WITH n, r LIMIT $batch_size
                    DELETE n, r
                    RETURN count(n) as deletedNodesCount
                    """, batch_size=batch_size)
                records = list(result)
                deleted_count = records[0]["deletedNodesCount"] if records else 0
    finally:
        driver.close()

###############################################################################
# Public - Transfer Functions
###############################################################################

def transfer(
    source_creds: Neo4jCredentials,
    target_creds: Neo4jCredentials,
    spec: TransferSpec,
) -> UploadResult:
    """Transfer data from one Neo4j instance to another.

    Args:
        source_creds: Credentials for the source Neo4j instance
        target_creds: Credentials for the target Neo4j instance
        spec: Specification for the data transfer

    Returns:
        UploadResult object with details of the upload
    """
    validate_credentials(source_creds)
    validate_credentials(target_creds)

    start_time = datetime.now()

    # Reset target database if specified
    if getattr(spec, 'reset_target', False):
        _reset_target_db(target_creds, spec.batch_size)

    src_driver = GraphDatabase.driver(
        source_creds.uri, 
        auth=(source_creds.username, source_creds.password)
    )
    tgt_driver = GraphDatabase.driver(
        target_creds.uri, 
        auth=(target_creds.username, target_creds.password)
    )

    try:
        with src_driver.session(database=source_creds.database) as src, \
             tgt_driver.session(database=target_creds.database) as tgt:
            
            logger.info("=== Copying nodes ===")
            node_labels = getattr(spec, 'node_labels', None)
            id_map = _copy_nodes(src, tgt, node_labels, spec.batch_size)
            logger.info(f"  → {len(id_map)} nodes copied")

            logger.info("=== Copying relationships ===")
            rel_types = getattr(spec, 'relationship_types', None)
            all_relationships = _copy_relationships(src, tgt, id_map, rel_types, spec.batch_size)
            logger.info(f"  → {len(all_relationships)} relationships copied")

            return UploadResult(
                started_at=start_time,
                records_total=len(id_map) + len(all_relationships),
                records_completed=len(id_map) + len(all_relationships),
                finished_at=datetime.now(),
                seconds_to_complete=(datetime.now() - start_time).total_seconds(),
                was_successful=True,
                nodes_created=len(id_map),
                relationships_created=len(all_relationships),
                properties_set=0,
            )
    finally:
        src_driver.close()
        tgt_driver.close()

def transfer_generator(
    source_creds: Neo4jCredentials,
    target_creds: Neo4jCredentials,
    spec: TransferSpec,
) -> Generator[UploadResult, None, None]:
    """Transfer data from one Neo4j instance to another with progress updates.

    Args:
        source_creds: Credentials for the source Neo4j instance
        target_creds: Credentials for the target Neo4j instance
        spec: Specification for the data transfer

    Yields:
        UploadResult objects with progress updates during the transfer
    """
    validate_credentials(source_creds)
    validate_credentials(target_creds)

    start_time = datetime.now()

    # Reset target database if specified
    if getattr(spec, 'reset_target', False):
        _reset_target_db(target_creds, spec.batch_size)

    src_driver = GraphDatabase.driver(
        source_creds.uri, 
        auth=(source_creds.username, source_creds.password)
    )
    tgt_driver = GraphDatabase.driver(
        target_creds.uri, 
        auth=(target_creds.username, target_creds.password)
    )

    try:
        with src_driver.session(database=source_creds.database) as src, \
             tgt_driver.session(database=target_creds.database) as tgt:
            
            logger.info("=== Copying nodes ===")
            node_labels = getattr(spec, 'node_labels', None)
            id_map = _copy_nodes(src, tgt, node_labels, spec.batch_size)
            nodes_copied = len(id_map)
            logger.info(f"  → {nodes_copied} nodes copied")

            # Yield progress after nodes are copied
            yield UploadResult(
                started_at=start_time,
                records_total=nodes_copied,  # Will be updated after relationships
                records_completed=nodes_copied,
                finished_at=None,
                seconds_to_complete=(datetime.now() - start_time).total_seconds(),
                was_successful=True,
                nodes_created=nodes_copied,
                relationships_created=0,
                properties_set=0,
            )

            logger.info("=== Copying relationships ===")
            rel_types = getattr(spec, 'relationship_types', None)
            all_relationships = _copy_relationships(src, tgt, id_map, rel_types, spec.batch_size)
            relationships_copied = len(all_relationships)
            logger.info(f"  → {relationships_copied} relationships copied")

            # Final yield with complete results
            yield UploadResult(
                started_at=start_time,
                records_total=nodes_copied + relationships_copied,
                records_completed=nodes_copied + relationships_copied,
                finished_at=datetime.now(),
                seconds_to_complete=(datetime.now() - start_time).total_seconds(),
                was_successful=True,
                nodes_created=nodes_copied,
                relationships_created=relationships_copied,
                properties_set=0,
            )
    finally:
        src_driver.close()
        tgt_driver.close()

def undo(creds: Neo4jCredentials, spec: TransferSpec):
    """Undo a transfer based on TransferSpec timestamp.

    Args:
        creds: Credentials for the Neo4j instance
        spec: Transfer specification containing timestamp information

    Returns:
        Query execution summary
    """
    timestamp_key = getattr(spec, 'timestamp_key', 'transfer_timestamp')
    timestamp_value = spec.timestamp.isoformat()
    
    driver = GraphDatabase.driver(creds.uri, auth=(creds.username, creds.password))
    try:
        with driver.session(database=creds.database) as session:
            query = f"""
            MATCH (n)
            WHERE n.`{timestamp_key}` = $datetime
            DETACH DELETE n
            RETURN count(n) as deletedCount
            """
            result = session.run(query, datetime=timestamp_value)
            
            # Get the data first, then consume
            record = result.single()
            deleted_count = record['deletedCount'] if record else 0
            summary = result.consume()
            
            logger.info(f"Undo operation completed: deleted {deleted_count} nodes")
            return summary
    finally:
        driver.close()

###############################################################################
# Public - Information Functions
###############################################################################

def get_node_labels(creds: Neo4jCredentials) -> list[str]:
    """Return a list of Node labels from a specified Neo4j instance.

    Args:
        creds (Neo4jCredential): Credentials object for Neo4j instance to get node labels from.

    Returns:
        list[str]: List of Node labels
    """
    result = []
    query = """
        call db.labels();
    """
    response, _, _ = execute_query(creds, query)

    logger.debug(f"get_nodes reponse: {response}")

    result = [r.data()["label"] for r in response]

    logger.info(f"Nodes found: {result}")
    return result


def get_relationship_types(creds: Neo4jCredentials) -> list[str]:
    """Return a list of Relationship types from a Neo4j instance.

    Args:
        creds (Neo4jCredential): Credentials object for Neo4j instance to get Relationship types from.

    Returns:
        list[str]: List of Relationship types
    """
    result = []
    query = """
        call db.relationshipTypes();
    """
    response, _, _ = execute_query(creds, query)

    logger.debug(f"get_relationships reponse: {response}")

    result = [r.data()["relationshipType"] for r in response]

    logger.info("Relationships found: " + str(result))
    return result

###############################################################################
# Public - Reset Target Database Functions
###############################################################################

def drop_target_db_constraints(
    tgt_sess: Session
):
    
    logger.info("=== Purging Constraints from Target DB ===")

    query = "SHOW CONSTRAINTS"
    result = tgt_sess.run(query)

    logger.debug(f"Drop constraints results: {result}")

    # Have to make a drop constraint request for each individually!
    for record in result:
        constraint_name = record.get("name", None)
        if constraint_name is not None:
            drop_query = f"DROP CONSTRAINT {constraint_name}"
            drop_result = tgt_sess.run(drop_query)
            logger.debug(f"Drop constraint {constraint_name} results: {drop_result}")

    # This should now show empty
    result = tgt_sess.run(query)
    return result

def drop_target_db_indexes(
    tgt_sess: Session
):
    logger.info("=== Purging Indexes from Target DB ===")
    query = "SHOW INDEXES"
    result = tgt_sess.run(query)

    # Have to make a drop index request for each individually!
    for record in result:
        index_name = record.get("name", None)
        if index_name is not None:
            drop_query = f"DROP INDEX {index_name} IF EXISTS"
            drop_result = tgt_sess.run(drop_query)

    return result

def reset_target_db(creds: Neo4jCredentials, batch_size: int = DEFAULT_BATCH_SIZE) -> None:

    logger.info("=== Resetting Target DB ===")

    drv_tgt: Driver = GraphDatabase.driver(creds.uri, auth=(creds.username, creds.password))
    try:
        with drv_tgt.session() as tgt:
            drop_target_db_constraints(tgt)
            drop_target_db_indexes(tgt)

            deleted_nodes_count = -1
            while deleted_nodes_count != 0:
                query = """
                MATCH (n)
                OPTIONAL MATCH (n)-[r]-()
                WITH n, r LIMIT $batch_size
                DELETE n, r
                RETURN count(n) as deletedNodesCount
                """
                result = tgt.run(query, batch_size=batch_size)
                records = list(result)
                deleted_nodes_count = records[0]["deletedNodesCount"] if records else 0
    finally:
        drv_tgt.close()