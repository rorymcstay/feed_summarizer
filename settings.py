import os


nanny_params = {
    "host": os.getenv("NANNY_HOST", "localhost"),
    "port": os.getenv("FLASK_PORT", 5003),
    "api_prefix": "containercontroller",
    "params_manager": "parametercontroller"
}

kafka_params = {
    "bootstrap_servers": [os.getenv("KAFKA_ADDRESS", "localhost:29092")]
}

subscribe_params = {
    "topics": os.getenv("TOPICS", "results")
}

hazelcast_params = {
    "host": os.getenv("HAZELCAST_HOST", "localhost"), "port": os.getenv("HAZELCAST_PORT", 5701)
}

database_parameters = {
    "host": os.getenv("DATABASE_HOST", "localhost"),
    "port": os.getenv("DATABASE_PORT", 5432),
    "database": os.getenv("DATABASE_NAME", "feeds"),
    "user": os.getenv("DATABASE_USER", "feeds"),
    "password": os.getenv("DATABASE_PASS", "feeds")
}

table_params = {
    "prefix": os.getenv("TABLE_PREFIX", "stg"),
    "type": os.getenv("TABLE_TYPE", "results"),
    "postfix": ""
}

