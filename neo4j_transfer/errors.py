class InvalidCredentialsError(Exception):
    """Error if Neo4j credentials are invalid"""

    pass


class InvalidPayloadError(Exception):
    """Error if json payload schema is unrecognized"""

    pass


class InvalidNodeLabelsError(Exception):
    """Error if node label(s) are invalid"""

    pass


class InvalidRelationshipTypesError(Exception):
    """Error if relationship type(s) are invalid"""

    pass


class MissingSourceNodeKey(Exception):
    """Error if unique node key missing"""

    pass


class MissingTargetNodeKey(Exception):
    """Error if unique node key missing"""

    pass


class MissingNodeKey(Exception):
    """Error if unique node key missing"""

    pass
