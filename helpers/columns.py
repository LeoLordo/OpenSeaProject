table_schema = ["collection", "name", "description", "image_url", "owner", "twitter_username", "contracts"]

table_schema_dict = {"id": "SERIAL PRIMARY KEY","collection": "VARCHAR",
                     "name": "VARCHAR",
                     "description": "TEXT",
                     "image_url": "VARCHAR",
                     "owner": "VARCHAR",
                     "twitter_username": "VARCHAR",
                     "contracts": "JSONB"}