from pymongo import MongoClient

class MongoConfig():
    def __init__(self, host, port, database, username, password):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

class MongoDB():
    def __init__(self, config):
        self.client = MongoClient(
            host=config.host,
            port=config.port,
            # username=config.username,
            # password=config.password,
            # authSource=config.database,
            # authMechanism='SCRAM-SHA-256'
        )
        # self.db = self.client[config.database]
        self.db = self.client

    def close(self):
        self.client.close()