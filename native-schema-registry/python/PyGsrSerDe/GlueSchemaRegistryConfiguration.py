
class GlueSchemaRegistryConfiguration:
    """An object containing important configuration parameters."""

    def __init__(self, config: dict):
        """Create a GlueSchemaRegistryConfiguration object."""
        self.config = config
        self.schema = self.config.get("schema")
        self.topic = self.config.get("topic")
        self.schema_naming_strategy = self.config.get("SchemaNamingStrategy")
        self.dataformat = self.config.get("dataformat")
