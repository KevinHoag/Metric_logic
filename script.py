from functools import wraps
from typing import Dict, List, Any, Callable, Type, get_type_hints
from datetime import datetime
import inspect
from db import MongoDB

class ValidateParams:
    def __init__(self, required_params_getter: Callable):
        self.required_params_getter = required_params_getter

    def validate_type(self, value: Any, expected_type: Type) -> bool:
        if expected_type == datetime and isinstance(value, str):
            try:
                datetime.fromisoformat(value)
                return True
            except ValueError:
                return False
        return isinstance(value, expected_type)

    def __call__(self, func):
        @wraps(func)
        def wrapper(instance, *args, **kwargs):
            params: Dict[str, Any] = kwargs
            
            # Get required params and their types from instance
            required_params = self.required_params_getter(instance)
            param_types = get_type_hints(instance.__class__)
            
            # Check for missing parameters
            missing_params = [
                param for param in required_params 
                if param not in params
            ]
            
            if missing_params:
                raise ValueError(
                    f"Missing required parameters: {', '.join(missing_params)}"
                )
            
            # Validate and assign parameters
            for key, value in params.items():
                if key in param_types:
                    if not self.validate_type(value, param_types[key]):
                        raise TypeError(
                            f"Parameter '{key}' must be of type {param_types[key].__name__}"
                        )
                setattr(instance, key, value)
                
            return func(instance, *args, **kwargs)
        return wrapper

class CustomerEvent:
    def __init__(self):
        self.required_params = ["params_visitDateFrom", "params_visitDateTo", "host", "port"]

    @ValidateParams(lambda self: self.required_params)
    def run(self, *args, **kwargs):
        mongo_client = MongoDB()
        mongo_client.setup_db(
            username=kwargs["username"],
            password=kwargs["password"],
            host=kwargs["host"],
            port=kwargs["port"],
            auth=kwargs["auth"],
        )

        # Query groups
        group_list = mongo_client.find(
            db_name=kwargs["db"],
            col_name="cam_groups",
            query={"group_id": {"$in": kwargs.get("params_groupIds", [])}}
        )["result"]

        # Query cameras
        camera_group_lists = [
            {
                "camera": mongo_client.find(
                    db_name=kwargs["db"],
                    col_name="cameras",
                    query={"group_id": group_id}
                )["result"],
                "group_id": group_id
            }
            for group_id in kwargs.get("params_groupIds", [])
        ]

        face_events_list = []
        for camera_group_list in camera_group_lists:
            camera_ids = [camera["camera_id"] for camera in camera_group_list["camera"]]
            face_events_query = mongo_client.aggregate(
                db_name=kwargs["db"],
                col_name="face_events",
                query=[
                    {
                        "$match": {
                            "camera_id": {"$in": camera_ids},
                            "timestamp": {
                                "$gte": datetime.fromisoformat(kwargs["params_visitDateFrom"]),
                                "$lte": datetime.fromisoformat(kwargs["params_visitDateTo"])
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": "$face_id",
                            "visit_count": {"$sum": 1},
                            "group_id": {"$first": camera_group_list["group_id"]}
                        }
                    }
                ]
            )["result"]

            face_events_list.extend(face_events_query)

        # Sorting and pagination
        face_events_list.sort(key=lambda x: x.get(kwargs["params_sortBy"]), reverse=kwargs.get("params_order", "asc") == "desc")
        face_events_list = face_events_list[0:int(kwargs["params_pageSize"]) * int(kwargs["params_page"])]

        face_id_lists = [item["_id"] for item in face_events_list]
        face_identities_list = mongo_client.find(
            db_name=kwargs["db"],
            col_name="face_identities",
            query={"face_id": {"$in": face_id_lists}}
        )["result"]

        for face_identity in face_identities_list:
            face_events_query = mongo_client.find(
                db_name=kwargs["db"],
                col_name="face_events",
                query={"face_id": face_identity["face_id"], "timestamp": face_identity["last_seen"]}
            )["result"]
            if face_events_query:
                face_identity["track_id"] = face_events_query[0].get("track_id")
                face_identity["event_id"] = face_events_query[0].get("event_id")

        results_face_event = face_events_list.copy()
        for item in results_face_event:
            for group in group_list:
                if item["group_id"] == group["group_id"]:
                    item["groupName"] = group["name"]
                    break
            for face_identity in face_identities_list:
                if item["_id"] == face_identity["face_id"]:
                    item["trackId"] = face_identity["track_id"]
                    item["eventId"] = face_identity["event_id"]
                    item["label"] = face_identity.get("labels")
                    item["lastVisitDay"] = (datetime.now() - face_identity["last_seen"]).days
                    item["lastVisitTime"] = face_identity["last_seen"].strftime("%Y-%m-%d %H:%M:%S")
                    item["fullName"] = face_identity["username"]
                    break

        return results_face_event



class ClassSerializer:
    @classmethod
    def serialize(cls, class_obj):
        class_definition = inspect.getsource(class_obj)
        return class_definition

    @classmethod
    def deserialize(cls, class_definition: str):
        exec(class_definition, globals())
        class_name = class_definition.split('class ')[1].split(':')[0].split('(')[0].strip()
        return globals()[class_name]

if __name__ == "__main__":
    # Example usage
    serialize_class = ClassSerializer.serialize(CustomerEvent)
    # print("Serialized Data:\n", serialize_class)

    # Deserialize the class definition
    deserialized_class = ClassSerializer.deserialize(serialize_class)
    # print("Deserialized Class:\n", inspect.getsource(deserialized_class))

    # Use the deserialized class
    report = deserialized_class()
    params = {
        "username": "",
        "password": "",
        "host": "localhost",
        "port": 27017,
        "db": "distill_db",
        "auth": "admin",
        "params_page": 1,
        "params_pageSize": 2,
        "params_search": "",
        "params_status": "identified",
        "params_sortBy": "visit_count",
        "params_order": "desc",
        "params_groupIds": ["CG-5", "CG-2"],
        "params_visitDateFrom": "2024-02-17T23:59:59.999Z",
        "params_visitDateTo": "2025-02-17T23:59:59.999Z",
        "params_tags": ["vip", "regular"]
    }

    res = report.run(**params)

    print(res)