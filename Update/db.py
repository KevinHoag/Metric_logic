import pymongo
import traceback

from typing import List
from pymongo import MongoClient


class MongoDB:
    client = None
    logger = None

    def setup_db(
            self,
            username: str,
            password: str,
            host: str,
            port: int,
            auth: str
        ):
        """_summary_

        Args:
            username (str): _description_
            password (str): _description_
            host (str): _description_
            port (int): _description_
            auth (str): _description_
        """        
        if auth is None:
            try:
                self.__class__.client = MongoClient(host=host,
                                                    port=port,
                                                    username=username,
                                                    password=password)
                self.logger.success("Initialize mongodb success")
            except Exception as e:
                print(f"Initialize mongodb fail with following error: {e}")
                
                # self.logger.error(f"Initialize mongodb fail with following error: {e}")
        else:
            try:
                self.__class__.client = MongoClient(host=host,
                                                    port=port,
                                                    username=username,
                                                    password=password,
                                                    authSource=auth)
                self.logger.success("Initialize mongodb success")
            except Exception as e:
                print(f"Initialize mongodb fail with following error: {e}")
                # self.logger.error(f"Initialize mongodb fail with following error: {e}")   

    @classmethod
    def insert_one(
            self,
            db_name: str,
            col_name: str,
            document: dict
        ):
        """_summary_

        Args:
            db_name (str): _description_
            col_name (str): _description_
            document (dict): _description_

        Returns:
            _type_: _description_
        """        
        col = self.client[db_name][col_name]
        try:
            col.insert_one(document)
            return {
                "status": True
            }
        except Exception as e:
            # self.logger.error(f"An exception occurred insert_one: {traceback.format_exc()}")
            return {
                "status": False,
                "error": e
            }

    @classmethod
    def insert_many(
            self,
            db_name: str,
            col_name: str,
            documents: List[dict]
        ):
        """_summary_

        Args:
            db_name (str): _description_
            col_name (str): _description_
            documents (List[dict]): _description_

        Returns:
            _type_: _description_
        """        
        col = self.client[db_name][col_name]
        try:
            col.insert_many(documents)
            return {
                "status": True,
            }
        except Exception as e:
            # self.logger.error(f"An exception occurred insert_many: {traceback.format_exc()}")
            return {
                "status": False,
                "error": e
            }
    
    @classmethod
    def drop_collection(
            self,
            db_name: str,
            col_name: str
        ):
        """_summary_

        Args:
            db_name (str): _description_
            col_name (str): _description_

        Returns:
            _type_: _description_
        """        
        try:
            collection_names = [n for n in self.client[db_name].list_collection_names()]
            if collection in collection_names:
                col = self.db[collection]
                col.drop()
                return {
                    "status": True
                }
            return {
                "status": True
            }

        except Exception as e:
            return {
                "status": False,
                "error": e
            }
    
    @classmethod
    def delete_one(
            self,
            db_name: str,
            col_name: str,
            query: str
        ):
        """_summary_

        Args:
            db_name (str): _description_
            col_name (str): _description_
            query (str): _description_

        Returns:
            _type_: _description_
        """        
        try:
            collection_names = [n for n in self.client[db_name].list_collection_names()]
            if col_name in collection_names:
                col = self.client[db_name][col_name]
                x = col.delete_one(query) 
                return {
                    "status": True
                }
            return {
                "status": True
            }
        except Exception as e:
            # self.logger.error(f"An exception occurred delete_one:{traceback.format_exc()}")
            return {
                "status": False,
                "error": e
            }
        
    @classmethod
    def delete_many(
            self,
            db_name: str,
            col_name: str,
            query: str
        ):
        """_summary_

        Args:
            db_name (str): _description_
            col_name (str): _description_
            query (str): _description_

        Returns:
            _type_: _description_
        """        
        try:
            collection_names = [n for n in self.client[db_name].list_collection_names()]
            if col_name in collection_names:
                col = self.client[db_name][col_name]
                x = col.delete_many(query)    
                return {
                    "status": True
                }
            return {
                "status": True
            }
        except Exception as e:
            # self.logger.error(f"An exception occurred: {traceback.format_exc()}")
            return {
                "status": False,
                "error": e
            }

    @classmethod    
    def find_one(
            self,
            db_name: str,
            col_name: str,
            query: str
        ):
        """_summary_

        Args:
            db_name (str): _description_
            col_name (str): _description_
            query (str): _description_

        Returns:
            _type_: _description_
        """        
        try:
            col = self.client[db_name][col_name]
            result = col.find_one(query)
            if result:
                return {
                    "status": True,
                    "result": result
                }
            else:
                return {
                    "status": False,
                    "result": []
                }
        except Exception as e:
            # self.logger.error(f"Find fail for query '{query}' with following error {traceback.format_exc()}")
            return {
                "status": False,
                "error": e
            }
    
    @classmethod
    def find(
            self,
            db_name: str,
            col_name: str,
            query: str,
            sort_data: list = None
        ):
        """_summary_

        Args:
            db_name (str): _description_
            col_name (str): _description_
            query (str): _description_

        Returns:
            _type_: _description_
        """        
        try:
            col = self.client[db_name][col_name]
            if sort_data is not None and len(sort_data)>0:
                result = list(col.find(query, {"_id": 0}).sort(sort_data))
            else:
                result = list(col.find(query, {"_id": 0}))
            if result:
                return {
                    "status": True,
                    "result": result
                }
            else:
                return {
                    "status": True,
                    "result": []
                }
        except Exception as e:
            # self.logger.error(f"An exception occurred: {traceback.format_exc()}")
            return {
                "status": False,
                "error": e
            }
        
    @classmethod
    def aggregate(
            self,
            db_name: str,
            col_name: str,
            query: List,
        ):
        """_summary_

        Args:
            db_name (str): _description_
            col_name (str): _description_
            query (str): _description_

        Returns:
            _type_: _description_
        """        
        try:
            col = self.client[db_name][col_name]
            result = list(col.aggregate(query))
            if result:
                return {
                    "status": True,
                    "result": result
                }
            else:
                return {
                    "status": True,
                    "result": []
                }
        except Exception as e:
            # self.logger.error(f"An exception occurred: {traceback.format_exc()}")
            return {
                "status": False,
                "error": e
            }

    @classmethod
    def update_one(
            self,
            db_name: str,
            col_name: str,
            query: str,
            document: dict
        ):
        """_summary_

        Args:
            db_name (str): _description_
            col_name (str): _description_
            query (str): _description_
            document (dict): _description_

        Returns:
            _type_: _description_
        """        
        col = self.client[db_name][col_name]
        try:
            newvalues = { "$set": document }
            col.update_one(query, newvalues)
            return {
                "status": True
            }
        except Exception as e:
            # self.logger.error(f"An exception occurred update_one: {traceback.format_exc()}")
            return {
                "status": False,
                "error": e
            }

    @classmethod
    def get_all_data(
            self,
            db_name: str,
            col_name: str
        ):         
        """_summary_

        Args:
            db_name (str): _description_
            col_name (str): _description_

        Returns:
            _type_: _description_
        """        
        try:
            col = self.client[db_name][col_name]  
            result = list(col.find())
            return result
        except Exception as e:
            # self.logger.error(f"An exception occurred get_all_data: {traceback.format_exc()}")
            return []
    
    @classmethod
    def update_or_insert_data(
            self, 
            db_name: str,
            col_name: str, 
            query: str, 
            document: dict
        ):
        """_summary_

        Args:
            db_name (str): _description_
            col_name (str): _description_
            query (str): _description_
            document (dict): _description_

        Returns:
            _type_: _description_
        """        
        try:
            col = self.client[db_name][col_name] 
            result = col.find_one(query)
            if result is not None:
                newvalues = { "$set": document }
                col.update_one(query, newvalues)
                return True
            else:
                self.insert_one(db_name=db_name, col_name=col_name, document=document)
                return True
        except Exception as e:
            # self.logger.error(f"An exception occurred update_or_insert_data: {traceback.format_exc()}")
            return False

    @classmethod
    def update_or_insert_data_many(
            self, 
            db_name, 
            col_name,
            query, 
            document
        ):
        try:
            col = self.client[db_name][col_name]
            result = col.find_one(query)
            if result is not None:
                newvalues = { "$set": document }
                col.update_many(query, newvalues)
                return {
                    "status": True
                }
            else:
                self.insert_document(db_name=db_name, col_name=col_name, document=document)
                return {
                    "status": True
                }
        except Exception as e:
            # self.logger.error(f"An exception occurred update_or_insert_data: {traceback.format_exc()}")
            return {
                "status": False,
                "error": e
            }

def initialize_mongodb(
        host: str,
        port: int,
        username: str,
        password: str,
        auth: str
    ):
    """_summary_

    Args:
        host (str): _description_
        port (int): _description_
        username (str): _description_
        password (str): _description_
        auth (str): _description_
    """    
    mdb = MongoDB()
    mdb.setup_db(username=username,
                 password=password,
                 host=host,
                 port=port,
                 auth=auth)