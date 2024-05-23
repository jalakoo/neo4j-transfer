from neo4j_transfer.models import Neo4jCredentials, TransferSpec, KeyTransferSpec
from neo4j_transfer.n4j import validate_credentials, execute_query
from neo4j_transfer._logger import logger
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
    """Transfer data from one Neo4j instance to another.

    Args:
        source_creds (Neo4jCredentials): Credentials for the source Neo4j instance
        target_creds (Neo4jCredentials): Credentials for the target Neo4j instance
        spec (TransferSpec): Specification for the data transfer

    Returns:
        A neo4j_uploader.models.UploadResult object with details of the upload
    """

    logger.info(f"Validating source database creds...")
    validate_credentials(source_creds)
    logger.info(f"Validating target database creds...")
    validate_credentials(target_creds)

    logger.info(f'Getting nodes from source database "{source_creds.uri}"...')
    nodes = get_nodes(source_creds, spec)
    logger.info(f"Node Specs for upload: {len(nodes)}")

    logger.info(f'Getting relationships from source database "{source_creds.uri}"...')
    rels = get_relationships(source_creds, spec)
    logger.info(f"Relationship Specs for upload: {len(rels)}")

    logger.info(f'Uploading data to target database "{target_creds.uri}"...')
    result = upload(target_creds, nodes, rels)

    logger.info(f"Upload complete! {result}")

    return result


def undo(creds: Neo4jCredentials, spec: TransferSpec):
    """
    Undo a transfer by deleting all nodes and relationships by timestamp specified in a TransferSpec.

    Args:
        creds (Neo4jCredentials): Credentials for the target Neo4j instance
        spec (TransferSpec): Specification used for the data transfer to undo
    """

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

        logger.debug(f"Getting {label} nodes...")

        nodes_query = f"""
            MATCH (n:`{label}`)
            RETURN n
            """
        records, _, _ = execute_query(creds, nodes_query)

        logger.debug(f"Number of {label} nodes found: {len(records)}")
        if len(records) > 0:
            logger.debug(f"First Node: {records[0]}")

        converted_records = []
        for n in records:
            node_raw = n.data()["n"]

            # Copy to editable dict
            node = dict(**node_raw)

            # Get specifications for transfering node data. Use default if no mapping found
            node_mapping = spec.node_mappings()
            if not isinstance(node_mapping, dict):
                raise ValueError(
                    f"Node mappings for {label} is not a dictionary. Found {type(node_mapping)}"
                )
            key_spec = node_mapping.get(label, KeyTransferSpec())
            sid_key = key_spec.source
            tid_key = key_spec.target

            if sid_key == "element_id":
                # Node element ids are not returned in the data() api
                node_element_id = n.values()[0]._element_id
                # KeyTransferSpec can specify what the target key will be (may not be the source key identifier)
                node.update({tid_key: node_element_id})
            else:
                # User specified keys, not using built-in element_ids
                uid = node.get(sid_key, None)
                if uid is None:
                    logger.warning(f"Node has no {sid_key} property. Skipping: {node}")
                    continue
                node.update({tid_key: uid})

            # remove original source node key id from data to transfer. Will be replaced with the tid
            node.pop(sid_key)

            # Append optional timestamp separately
            if spec.timestamp_key is not None:
                node.update(
                    {
                        spec.timestamp_key: spec.timestamp.isoformat(),
                    }
                )

            converted_records.append(node)

        nodes_spec = Nodes(labels=[label], key=tid_key, records=converted_records)

        result.append(nodes_spec)

    return result


def get_relationships(
    creds: Neo4jCredentials, spec: TransferSpec
) -> list[Relationships]:
    """Retrieve Relationships from source and format for uploading"""

    result = []
    r_types = spec.relationship_mappings().keys()
    for r_type in r_types:
        logger.debug(f'Processing relationships of type "{r_type}"...')

        query = """
            MATCH (n)-[r]->(n2)
            WHERE any(label IN labels(n) WHERE label IN $labels) AND any(label IN labels(n2) WHERE label IN $labels) AND type(r) in $types
            RETURN n, r, n2
        """
        node_labels = list(spec.node_mappings().keys())
        params = {"labels": node_labels, "types": [r_type]}
        records, _, _ = execute_query(creds, query, params)

        logger.debug(f"Number of {r_type} relationships found: {len(records)}")
        # Going to sort in groups of source-targest-relationshipTypes : Relationship objects
        sorter = {}
        for r in records:

            n_labels = list(r.values()[0].labels)
            n2_labels = list(r.values()[2].labels)
            # rel_type and r_type should be the same
            if r_type != r.values()[1].type:
                raise ValueError(
                    f"Relationship type {r_type} from mappings does not match relationship type {r.values()[1].type} from db records"
                )
            # r_type = r.values()[1].type
            key = f"{n_labels} : {n2_labels} : {r_type}"

            # Using only 1 label from each source & target node to key configuration data
            from_node_label = n_labels[0]
            to_node_label = n2_labels[0]

            # Getting mapping data for that label
            from_key_spec = spec.node_mappings().get(from_node_label, None)
            if from_key_spec is None:
                raise ValueError(f"No node mapping found for label: {from_node_label}")
            to_key_spec = spec.node_mappings().get(to_node_label, None)
            if to_key_spec is None:
                raise ValueError(f"No node mapping found for label: {to_node_label}")

            # Get source Node keys to use
            from_key = from_key_spec.source
            to_key = to_key_spec.source

            # Get the Node keys in the duplicated Nodes data
            from_transfer_key = from_key_spec.target
            to_transfer_key = to_key_spec.target

            # Create target node keys for the relationship records only
            from_target_key = (
                f"_from_{from_key_spec.target}"
                if from_key_spec.target is not None
                else f"_from_{from_key}"
            )
            to_target_key = (
                f"_to_{to_key_spec.target}"
                if to_key_spec.target is not None
                else f"_to_{to_key}"
            )

            # Get node key-values
            from_node_values = dict(**r.values()[0]._properties)
            to_node_values = dict(**r.values()[2]._properties)
            if not isinstance(from_node_values, dict):
                raise ValueError(
                    f"Source Node values are not a dictionary. Got: {from_node_values}"
                )
            if not isinstance(to_node_values, dict):
                raise ValueError(
                    f"Target Node values are not a dictionary. Got: {to_node_values}"
                )

            # Exception for element id, which exists outside of properties data
            if from_key == "element_id":
                from_key_value = r.values()[0]._element_id
            else:
                from_key_value = from_node_values.get(from_key, None)

            if to_key == "element_id":
                to_key_value = r.values()[2]._element_id
            else:
                to_key_value = to_node_values.get(to_key, None)

            if from_key_value is None or to_key_value is None:
                logger.warning(
                    f"Relationship has no {from_key} or {to_key} property. Skipping: {r}"
                )
                continue

            # Create a relationship dictionary from properties
            record_dict = dict(**r.values()[1]._properties)

            # TODO: Add relationship unique id
            record_dict.update(
                {
                    from_target_key: from_key_value,
                    to_target_key: to_key_value,
                }
            )

            if spec.timestamp_key is not None:
                record_dict.update(
                    {
                        spec.timestamp_key: spec.timestamp.isoformat(),
                    }
                )

            if key in sorter:
                # Append new data to existing Relationships record
                sorter[key].records.append(record_dict)
            else:
                # Create a new Relationships record
                source_node = TargetNode(
                    node_label=from_node_label,
                    node_key=from_transfer_key,
                    record_key=from_target_key,
                )
                target_node = TargetNode(
                    node_label=to_node_label,
                    node_key=to_transfer_key,
                    record_key=to_target_key,
                )
                sorter[key] = Relationships(
                    type=r_type,
                    from_node=source_node,
                    to_node=target_node,
                    records=[record_dict],
                )

        logger.debug(f"Sorted relationship combinations: {sorter.keys()}")

        # Convert to list of Relationship objects
        for value in sorter.values():
            # Each record will now be a Neo4jUploader Relationships Object
            result.append(value)

        logger.debug(f"Relationship Specifications generated: {len(result)}")

        # source_node = TargetNode(
        #     node_label=None,
        #     node_key=spec.element_id_key,
        #     record_key=f"_from_{spec.element_id_key}",
        # )
        # target_node = TargetNode(
        #     node_label=None,
        #     node_key=spec.element_id_key,
        #     record_key=f"_to_{spec.element_id_key}",
        # )

        # converted_records = []
        # for rec in records:
        #     from_eid = rec.values()[0].element_id
        #     to_eid = rec.values()[2].element_id
        #     r_eid = rec.values()[1].element_id

        #     # TODO: Relationship properties

        #     # Required data to connect relationships with source and target nodes
        #     rel_rec = {
        #         f"_from_{spec.element_id_key}": from_eid,
        #         f"_to_{spec.element_id_key}": to_eid,
        #     }

        #     if spec.should_append_data:
        #         # Add default transfer related data
        #         rel_rec.update(
        #             {
        #                 spec.element_id_key: r_eid,
        #                 spec.timestamp_key: spec.timestamp.isoformat(),
        #             }
        #         )
        #     converted_records.append(rel_rec)

        # rels_spec = Relationships(
        #     type=type,
        #     from_node=source_node,
        #     to_node=target_node,
        #     records=converted_records,
        # )
        # result.append(rels_spec)

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
