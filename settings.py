import os

table_params = {
    "prefix": os.getenv("TABLE_PREFIX", "stg"),
    "type": os.getenv("TABLE_TYPE", "results"),
    "postfix": ""
}

