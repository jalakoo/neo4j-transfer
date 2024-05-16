from neo4j_transfer.models import Neo4jCredentials, TransferSpec
from neo4j_transfer.n4j import validate_credentials, execute_query
from neo4j_transfer._logger import logger
import neo4j_transfer.errors as errors_
from neo4j_uploader import batch_upload
from neo4j_uploader.models import (
    Relationships,
    TargetNode,
    Nodes,
    Neo4jConfig,
    GraphData,
)
from datetime import datetime


def transfer(
    source_creds: Neo4jCredentials, target_creds: Neo4jCredentials, spec: TransferSpec
):
    """Transfer data from one Neo4j instance to another.

    Args:
        source_creds (Neo4jCredentials): Credentials for the source Neo4j instance
        target_creds (Neo4jCredentials): Credentials for the target Neo4j instance
        spec (TransferSpec): Specification for the data transfer

    Returns:
        A neo4j_uploader.models.UploadResult object with details of the upload
    """

    validate_credentials(source_creds)
    validate_credentials(target_creds)

    nodes = get_nodes(source_creds, spec)
    rels = get_relationships(source_creds, spec)

    result = upload(target_creds, nodes, rels)

    return result


def undo(creds: Neo4jCredentials, spec: TransferSpec):

    timestamp_key = spec.timestamp_key
    ds_string = f"{spec.timestamp.isoformat()}"
    query = f"""
    MATCH (n)
    WHERE n.`{timestamp_key}` = $datetime
    DETACH DELETE n
    """
    params = {"datetime": ds_string}
    _, summary, _ = execute_query(creds, query, params)

    return summary


def upload(
    creds: Neo4jCredentials, nodes: list[Nodes], relationships: list[Relationships]
):

    n4j_config = Neo4jConfig(
        neo4j_uri=creds.uri,
        neo4j_user=creds.username,
        neo4j_password=creds.password,
        overwrite=False,
    )
    graph_data = GraphData(nodes=nodes, relationships=relationships)

    result = batch_upload(n4j_config, graph_data)

    return result


def get_nodes(creds: Neo4jCredentials, spec: TransferSpec) -> list[Nodes]:
    """Retrieve Nodes from source and format for uploading"""

    # Get nodes and convert to upload format
    result = []
    for label in spec.node_labels:

        nodes_query = f"""
            MATCH (n:`{label}`)
            RETURN n
            """
        records, _, _ = execute_query(creds, nodes_query)

        logger.info(f"\n Number of {label} nodes: {len(records)}")
        if len(records) > 0:
            logger.info(f"\n First Node: {records[0]}")

        converted_records = []
        for n in records:
            node_raw = n.data()["n"]

            # Copy to editable dict
            node = dict(**node_raw)

            if spec.should_append_data:
                # Get original element id if specified in transfer spec
                element_id = n.values()[0].element_id

                # Add default transfer related data
                node.update(
                    {
                        spec.element_id_key: element_id,
                        spec.timestamp_key: spec.timestamp.isoformat(),
                    }
                )

            converted_records.append(node)

        nodes_spec = Nodes(
            labels=[label], key=spec.element_id_key, records=converted_records
        )

        result.append(nodes_spec)

    return result


def get_relationships(
    creds: Neo4jCredentials, spec: TransferSpec
) -> list[Relationships]:
    """Retrieve Relationships from source and format for uploading"""

    result = []
    for type in spec.relationship_types:
        query = """
            MATCH (n)-[r]->(n2)
            WHERE any(label IN labels(n) WHERE label IN $labels) AND any(label IN labels(n2) WHERE label IN $labels) AND type(r) in $types
            RETURN n, r, n2
        """
        params = {"labels": spec.node_labels, "types": [type]}
        records, _, _ = execute_query(creds, query, params)

        source_node = TargetNode(
            node_label=None,
            node_key=spec.element_id_key,
            record_key=f"_from_{spec.element_id_key}",
        )
        target_node = TargetNode(
            node_label=None,
            node_key=spec.element_id_key,
            record_key=f"_to_{spec.element_id_key}",
        )

        converted_records = []
        for rec in records:
            from_eid = rec.values()[0].element_id
            to_eid = rec.values()[2].element_id
            r_eid = rec.values()[1].element_id

            # TODO: Relationship properties

            # Required data to connect relationships with source and target nodes
            rel_rec = {
                f"_from_{spec.element_id_key}": from_eid,
                f"_to_{spec.element_id_key}": to_eid,
            }

            if spec.should_append_data:
                # Add default transfer related data
                rel_rec.update(
                    {
                        spec.element_id_key: r_eid,
                        spec.timestamp_key: spec.timestamp.isoformat(),
                    }
                )
            converted_records.append(rel_rec)

        rels_spec = Relationships(
            type=type,
            from_node=source_node,
            to_node=target_node,
            records=converted_records,
        )
        result.append(rels_spec)

    return result


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
