import os

subscribe_params = {
    "topics": os.getenv("TOPIC_TYPE", "results")
}


table_params = {
    "stgprefix": os.getenv("STG_TABLE_PREFIX", "stg"),
    "fctprefix": os.getenv("FCT_TABLE_PREFIX", 'fct'),
    "type": os.getenv("TABLE_TYPE", "results"),
    "postfix": ""
}

