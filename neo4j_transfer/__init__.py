from .models import Neo4jCredentials, TransferSpec, UploadResult
from .n4j import validate_credentials, execute_query
from ._logger import logger
from neo4j import GraphDatabase, Driver, Session
from typing import List, Dict, Any, Generator, Union, Optional
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
    page_size: int = DEFAULT_BATCH_SIZE,
    progress_callback: callable = None,
    should_append_data: bool = False,
    element_id_key: str = "_transfer_element_id",
    timestamp_key: str = "_transfer_timestamp",
    timestamp: Optional[datetime] = None,
) -> Dict[str, str]:
    """Copy nodes with specified labels."""
    id_map: Dict[str, str] = {}
    skip = 0
    total_processed = 0
    
    # Get total count for progress tracking
    if labels:
        count_query = """
        MATCH (n)
        WHERE any(label IN labels(n) WHERE label IN $labels)
        RETURN count(n) as total
        """
        total_nodes = src.run(count_query, labels=labels).single()["total"]
    else:
        total_nodes = src.run("MATCH (n) RETURN count(n) as total").single()["total"]
    
    logger.info(f"Starting to copy {total_nodes} nodes...")
    
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
        if should_append_data and batch:
            ts_value = (timestamp or datetime.now()).isoformat()
            for row in batch:
                props = row.get("props", {}) or {}
                # Append source element id and timestamp to properties
                props[element_id_key] = row.get("eid")
                props[timestamp_key] = ts_value
                row["props"] = props
        if not batch:
            break
        
        _batch_create_nodes(tgt, batch, id_map)
        batch_size = len(batch)
        total_processed += batch_size
        skip += page_size
        
        # Log progress
        progress_pct = (total_processed / total_nodes) * 100 if total_nodes > 0 else 100
        logger.info(f"    … Processed {total_processed}/{total_nodes} nodes ({progress_pct:.1f}%)")
        
        # If callback provided, call it with progress info
        if progress_callback:
            progress_callback("nodes", total_processed, total_nodes, batch_size)
    
    logger.info(f"Completed copying {total_processed} nodes")
    return id_map

def _copy_relationships(
    src: Session,
    tgt: Session,
    id_map: Dict[str, str],
    types: List[str] = None,
    page_size: int = DEFAULT_BATCH_SIZE,
    progress_callback: callable = None,
    should_append_data: bool = False,
    element_id_key: str = "_transfer_element_id",
    timestamp_key: str = "_transfer_timestamp",
    timestamp: Optional[datetime] = None,
) -> List[Dict]:
    """Copy relationships with optional type filtering."""
    all_relationships: List[Dict] = []
    skip = 0
    total_processed = 0
    
    # Get total count for progress tracking
    if types:
        count_query = """
        MATCH ()-[r]->()
        WHERE type(r) IN $types
        RETURN count(r) as total
        """
        total_rels = src.run(count_query, types=types).single()["total"]
    else:
        total_rels = src.run("MATCH ()-[r]->() RETURN count(r) as total").single()["total"]
    
    logger.info(f"Starting to copy {total_rels} relationships...")

    while True:
        if types:
            result = src.run(
                """
                MATCH (a)-[r]->(b)
                WHERE type(r) IN $types
                WITH r, a, b SKIP $skip LIMIT $limit
                RETURN type(r) AS type, elementId(r) AS eid, elementId(a) AS start, elementId(b) AS end, properties(r) AS props
                """,
                skip=skip, limit=page_size, types=types
            )
        else:
            result = src.run(
                """
                MATCH (a)-[r]->(b)
                WITH r, a, b SKIP $skip LIMIT $limit
                RETURN type(r) AS type, elementId(r) AS eid, elementId(a) AS start, elementId(b) AS end, properties(r) AS props
                """,
                skip=skip, limit=page_size
            )

        batch = []
        for rec in result:
            start_tgt = id_map.get(rec["start"])
            end_tgt = id_map.get(rec["end"])
            if start_tgt and end_tgt:
                props = rec.get("props", {}) or {}
                if should_append_data:
                    ts_value = (timestamp or datetime.now()).isoformat()
                    props[element_id_key] = rec.get("eid")
                    props[timestamp_key] = ts_value
                batch.append({
                    "type": rec["type"],
                    "start": start_tgt,
                    "end": end_tgt,
                    "props": props,
                })

        if not batch:
            break

        batch_relationships = _batch_create_relationships(tgt, batch)
        all_relationships.extend(batch_relationships)
        batch_size = len(batch_relationships)
        total_processed += batch_size
        skip += page_size
        
        # Log progress
        progress_pct = (total_processed / total_rels) * 100 if total_rels > 0 else 100
        logger.info(f"    … Processed {total_processed}/{total_rels} relationships ({progress_pct:.1f}%)")
        
        # If callback provided, call it with progress info
        if progress_callback:
            progress_callback("relationships", total_processed, total_rels, batch_size)
    
    logger.info(f"Completed copying {total_processed} relationships")
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

class StopTransfer(Exception):
    """Raised when a transfer is requested to stop."""
    pass

class StoppableTransfer:
    def __init__(self):
        self._stop_requested = False
    
    def request_stop(self):
        """Request the transfer to stop at the next opportunity."""
        self._stop_requested = True
    
    def check_stop(self):
        """Raise StopTransfer if a stop has been requested."""
        if self._stop_requested:
            raise StopTransfer("Transfer was stopped by user request")

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
    # Get the generator
    transfer_gen = transfer_generator(source_creds, target_creds, spec)
    
    # First item is the controller, but we don't need it in this synchronous version
    _ = next(transfer_gen)
    
    # Consume all progress updates and keep the last one (final result)
    result = None
    try:
        for update in transfer_gen:
            result = update
    except StopIteration:
        pass
    
    # Return the final result
    if result is None:
        raise RuntimeError("No results were generated by the transfer process")
    return result

def transfer_generator(
    source_creds: Neo4jCredentials,
    target_creds: Neo4jCredentials,
    spec: TransferSpec,
) -> Generator[Union[UploadResult, StoppableTransfer], None, None]:
    """Transfer data from one Neo4j instance to another with progress updates.
    
    Yields:
        UploadResult: Progress updates during the transfer
        StoppableTransfer: A controller that can be used to stop the transfer
    """
    validate_credentials(source_creds)
    validate_credentials(target_creds)

    start_time = datetime.now()
    # Set a single transfer timestamp so all batches share the same value
    transfer_timestamp = getattr(spec, 'timestamp', None) or start_time
    try:
        # Ensure spec reflects the actual transfer timestamp (useful for undo)
        setattr(spec, 'timestamp', transfer_timestamp)
    except Exception:
        pass
    progress = {
        'nodes_copied': 0,
        'total_nodes': 0,
        'relationships_copied': 0,
        'total_relationships': 0
    }
    
    # Create a stoppable controller
    controller = StoppableTransfer()
    yield controller  # First yield the controller

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
            
            controller.check_stop()
            logger.info("=== Copying nodes ===")
            node_labels = getattr(spec, 'node_labels', None)
            
            def node_progress(entity_type, processed, total, batch_size):
                controller.check_stop()
                progress['nodes_copied'] = processed
                progress['total_nodes'] = total
                yield UploadResult(
                    started_at=start_time,
                    records_total=total,
                    records_completed=processed,
                    finished_at=None,
                    seconds_to_complete=(datetime.now() - start_time).total_seconds(),
                    was_successful=True,
                    nodes_created=processed,
                    relationships_created=0,
                    properties_set=0,
                )

            id_map = _copy_nodes(
                src,
                tgt,
                node_labels,
                spec.batch_size,
                progress_callback=node_progress,
                should_append_data=getattr(spec, 'should_append_data', False),
                element_id_key=getattr(spec, 'element_id_key', '_transfer_element_id'),
                timestamp_key=getattr(spec, 'timestamp_key', '_transfer_timestamp'),
                timestamp=transfer_timestamp,
            )
            
            controller.check_stop()
            logger.info("=== Copying relationships ===")
            rel_types = getattr(spec, 'relationship_types', None)
            
            def rel_progress(entity_type, processed, total, batch_size):
                controller.check_stop()
                progress['relationships_copied'] = processed
                progress['total_relationships'] = total
                yield UploadResult(
                    started_at=start_time,
                    records_total=progress['total_nodes'] + total,
                    records_completed=progress['nodes_copied'] + processed,
                    finished_at=None,
                    seconds_to_complete=(datetime.now() - start_time).total_seconds(),
                    was_successful=True,
                    nodes_created=progress['nodes_copied'],
                    relationships_created=processed,
                    properties_set=0,
                )

            all_relationships = _copy_relationships(
                src,
                tgt,
                id_map,
                rel_types,
                spec.batch_size,
                progress_callback=rel_progress,
                should_append_data=getattr(spec, 'should_append_data', False),
                element_id_key=getattr(spec, 'element_id_key', '_transfer_element_id'),
                timestamp_key=getattr(spec, 'timestamp_key', '_transfer_timestamp'),
                timestamp=transfer_timestamp,
            )
            
            # Final result with complete information
            yield UploadResult(
                started_at=start_time,
                records_total=progress['nodes_copied'] + len(all_relationships),
                records_completed=progress['nodes_copied'] + len(all_relationships),
                finished_at=datetime.now(),
                seconds_to_complete=(datetime.now() - start_time).total_seconds(),
                was_successful=True,
                nodes_created=progress['nodes_copied'],
                relationships_created=len(all_relationships),
                properties_set=0,
            )
    except StopTransfer:
        logger.info("Transfer was stopped by user request")
        yield UploadResult(
            started_at=start_time,
            records_total=0,
            records_completed=0,
            finished_at=datetime.now(),
            seconds_to_complete=(datetime.now() - start_time).total_seconds(),
            was_successful=False,
            nodes_created=0,
            relationships_created=0,
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

def get_node_and_relationship_counts(
    creds: Neo4jCredentials,
    node_labels: list[str],
    relationship_types: list[str]
) -> tuple[int, int]:
    """Count nodes and relationships where relationships connect filtered nodes.
    
    Args:
        creds (Neo4jCredential): Credentials object for Neo4j instance to get node and relationship counts from.
        node_labels (list[str]): List of Node labels to count.
        relationship_types (list[str]): List of Relationship types to count. Only relationships connecting nodes with the specified labels will be counted.
    
    Returns:
        tuple[int, int]: Tuple of Node and Relationship counts (node count, relationship count)
    """
    
    # Build the base pattern
    node_filter = ""
    rel_filter = ""
    params = {}
    
    if node_labels:
        node_filter = "WHERE any(label IN labels(n) WHERE label IN $node_labels) AND any(label IN labels(m) WHERE label IN $node_labels)"
        params["node_labels"] = node_labels
    
    if relationship_types:
        rel_filter = "AND type(r) IN $relationship_types" if node_filter else "WHERE type(r) IN $relationship_types"
        params["relationship_types"] = relationship_types
    
    query = f"""
    MATCH (n)-[r]->(m)
    {node_filter}
    {rel_filter}
    WITH collect(DISTINCT n) + collect(DISTINCT m) AS nodes, count(r) AS relCount
    RETURN size(nodes) AS nodeCount, relCount AS relationshipCount
    """
    
    try:
        response, _, _ = execute_query(creds, query, params)
        if response:
            result = response[0]
            return int(result.get("nodeCount", 0)), int(result.get("relationshipCount", 0))
        return 0, 0
    except Exception as e:
        logger.warning(f"Error getting counts: {e}")
        return 0, 0

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

def drop_target_db_constraints(tgt_sess: Session) -> List[str]:
    """Drop all constraints from the target database.
    
    Args:
        tgt_sess: Neo4j session for the target database
        
    Returns:
        List of dropped constraint names
    """
    logger.info("=== Purging Constraints from Target DB ===")
    
    dropped_constraints = []
    
    try:
        # Get all constraints
        result = tgt_sess.run("SHOW CONSTRAINTS")
        constraints = list(result)
        
        if not constraints:
            logger.info("No constraints found to drop")
            return dropped_constraints
            
        logger.info(f"Found {len(constraints)} constraints to drop")
        
        # Drop each constraint
        for record in constraints:
            constraint_name = record.get("name")
            if constraint_name:
                try:
                    tgt_sess.run(f"DROP CONSTRAINT `{constraint_name}` IF EXISTS")
                    dropped_constraints.append(constraint_name)
                    logger.debug(f"Dropped constraint: {constraint_name}")
                except Exception as e:
                    logger.warning(f"Failed to drop constraint {constraint_name}: {e}")
                    
        logger.info(f"Successfully dropped {len(dropped_constraints)} constraints")
        
    except Exception as e:
        logger.error(f"Error during constraint dropping: {e}")
        raise
        
    return dropped_constraints

def drop_target_db_indexes(tgt_sess: Session) -> List[str]:
    """Drop all indexes from the target database.
    
    Args:
        tgt_sess: Neo4j session for the target database
        
    Returns:
        List of dropped index names
    """
    logger.info("=== Purging Indexes from Target DB ===")
    
    dropped_indexes = []
    
    try:
        # Get all indexes
        result = tgt_sess.run("SHOW INDEXES")
        indexes = list(result)
        
        if not indexes:
            logger.info("No indexes found to drop")
            return dropped_indexes
            
        logger.info(f"Found {len(indexes)} indexes to drop")
        
        # Drop each index
        for record in indexes:
            index_name = record.get("name")
            if index_name:
                try:
                    tgt_sess.run(f"DROP INDEX `{index_name}` IF EXISTS")
                    dropped_indexes.append(index_name)
                    logger.debug(f"Dropped index: {index_name}")
                except Exception as e:
                    logger.warning(f"Failed to drop index {index_name}: {e}")
                    
        logger.info(f"Successfully dropped {len(dropped_indexes)} indexes")
        
    except Exception as e:
        logger.error(f"Error during index dropping: {e}")
        raise
        
    return dropped_indexes

def delete_all_data(tgt_sess: Session, batch_size: int = DEFAULT_BATCH_SIZE) -> int:
    """Delete all nodes and relationships from the database in batches.
    
    Args:
        tgt_sess: Neo4j session for the target database
        batch_size: Number of nodes to delete per batch
        
    Returns:
        Total number of nodes deleted
    """
    logger.info("=== Deleting All Data from Target DB ===")
    
    total_deleted = 0
    batch_count = 0
    
    try:
        while True:
            # Delete nodes and relationships in batches
            query = """
            MATCH (n)
            WITH n LIMIT $batch_size
            OPTIONAL MATCH (n)-[r]-()
            DELETE n, r
            RETURN count(n) as deletedCount
            """
            
            result = tgt_sess.run(query, batch_size=batch_size)
            record = result.single()
            deleted_count = record["deletedCount"] if record else 0
            
            if deleted_count == 0:
                break
                
            total_deleted += deleted_count
            batch_count += 1
            
            logger.debug(f"Batch {batch_count}: deleted {deleted_count} nodes")
            
        logger.info(f"Successfully deleted {total_deleted} nodes total")
        
    except Exception as e:
        logger.error(f"Error during data deletion: {e}")
        raise
        
    return total_deleted

def reset_target_db(creds: Neo4jCredentials, batch_size: int = DEFAULT_BATCH_SIZE) -> Dict[str, Any]:
    """Reset the target database by dropping all constraints, indexes, and data.
    
    Args:
        creds: Neo4j credentials for the target database
        batch_size: Batch size for data deletion
        
    Returns:
        Dictionary with reset operation results
    """
    logger.info("=== Starting Target DB Reset ===")
    
    results = {
        "constraints_dropped": [],
        "indexes_dropped": [],
        "nodes_deleted": 0,
        "success": False
    }
    
    driver = None
    try:
        driver = GraphDatabase.driver(
            creds.uri, 
            auth=(creds.username, creds.password)
        )
        
        with driver.session(database=creds.database) as session:
            # Drop constraints first (they may depend on indexes)
            results["constraints_dropped"] = drop_target_db_constraints(session)
            
            # Drop indexes
            results["indexes_dropped"] = drop_target_db_indexes(session)
            
            # Delete all data
            results["nodes_deleted"] = delete_all_data(session, batch_size)
            
            results["success"] = True
            
        logger.info("=== Target DB Reset Complete ===")
        logger.info(f"Summary: {len(results['constraints_dropped'])} constraints, "
                   f"{len(results['indexes_dropped'])} indexes, "
                   f"{results['nodes_deleted']} nodes deleted")
                   
    except Exception as e:
        logger.error(f"Failed to reset target database: {e}")
        results["error"] = str(e)
        raise
        
    finally:
        if driver:
            driver.close()
            
    return results

def verify_database_empty(creds: Neo4jCredentials) -> Dict[str, int]:
    """Verify that the database is empty after reset.
    
    Args:
        creds: Neo4j credentials for the database
        
    Returns:
        Dictionary with counts of remaining objects
    """
    driver = None
    try:
        driver = GraphDatabase.driver(
            creds.uri, 
            auth=(creds.username, creds.password)
        )
        
        with driver.session(database=creds.database) as session:
            # Count nodes
            node_result = session.run("MATCH (n) RETURN count(n) as nodeCount")
            node_count = node_result.single()["nodeCount"]
            
            # Count relationships
            rel_result = session.run("MATCH ()-[r]-() RETURN count(r) as relCount")
            rel_count = rel_result.single()["relCount"]
            
            # Count constraints
            const_result = session.run("SHOW CONSTRAINTS")
            constraint_count = len(list(const_result))
            
            # Count indexes
            idx_result = session.run("SHOW INDEXES")
            index_count = len(list(idx_result))
            
            verification = {
                "nodes": node_count,
                "relationships": rel_count,
                "constraints": constraint_count,
                "indexes": index_count
            }
            
            if any(count > 0 for count in verification.values()):
                logger.warning(f"Database not completely empty: {verification}")
            else:
                logger.info("Database verification: completely empty")
                
            return verification
            
    except Exception as e:
        logger.error(f"Error verifying database state: {e}")
        raise
    finally:
        if driver:
            driver.close()