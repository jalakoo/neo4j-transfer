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


def transfer(
    source_creds: Neo4jCredentials, target_creds: Neo4jCredentials, spec: TransferSpec
):
    """Transfer data from one Neo4j instance to another"""

    validate_credentials(source_creds)
    validate_credentials(target_creds)

    nodes = get_nodes(source_creds, spec)
    rels = get_relationships(source_creds, spec)

    # logger.info(f"Uploading {len(nodes)} nodes specifications : {nodes}\n\nRelationships: {rels}")

    result = upload(target_creds, nodes, rels)

    return result


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
    """Retrieve nodes from source and format for uploading"""

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

        records = []
        for n in records:
            node = n.data()["n"]
            # Add original element id if specified in transfer spec
            element_id = n.values()[0].element_id
            node.update({spec.transfer_key: element_id})

            records.append(node)

        nodes_spec = Nodes(labels=[label], key=spec.transfer_key, records=records)

        result.append(nodes_spec)
    return result


def get_relationships(
    creds: Neo4jCredentials, spec: TransferSpec
) -> list[Relationships]:

    result = []
    for type in spec.relationship_types:
        query = """
            MATCH (n)-[r]-(n2)
            WHERE any(label IN labels(n) WHERE label IN $labels) AND any(label IN labels(n2) WHERE label IN $labels) AND type(r) in $types
            RETURN n, r, n2
        """
        params = {"labels": spec.node_labels, "types": [type]}
        records, _, _ = execute_query(creds, query, params)

        source_node = TargetNode(
            node_label=None,
            node_key=spec.transfer_key,
            record_key=f"_from_{spec.transfer_key}",
        )
        target_node = TargetNode(
            node_label=None,
            node_key=spec.transfer_key,
            record_key=f"_to_{spec.transfer_key}",
        )

        for rec in records:
            from_eid = rec.values()[0].element_id
            to_eid = rec.values()[2].element_id
            r_eid = rec.values()[1].element_id
            # TODO: Relationship properties
            rel_rec = {
                f"_from_{spec.transfer_key}": from_eid,
                f"_to_{spec.transfer_key}": to_eid,
                spec.transfer_key: r_eid,
            }

        rels_spec = Relationships(
            type=type,
            from_node=source_node,
            to_node=target_node,
            records=[rel_rec],
        )
        result.append(rels_spec)
    return result


def get_node_labels(creds: Neo4jCredentials) -> list[str]:
    """Return a list of Node labels from a Neo4j instance

    Args:
        creds (Neo4jCredential): Credentials object

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
    """Return a list of Relationship types from a Neo4j instance

    Args:
        creds (Neo4jCredential): Credentials object

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
