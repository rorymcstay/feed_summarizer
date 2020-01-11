import os

subscribe_params = {
    "topics": os.getenv("TOPIC_TYPE", "results")
}


table_params = {
    "prefix": os.getenv("TABLE_PREFIX", "stg"),
    "type": os.getenv("TABLE_TYPE", "results"),
    "postfix": ""
}

