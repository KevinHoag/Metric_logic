from functools import wraps
from typing import Dict, List, Any, Callable, Type, get_type_hints
import datetime
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
    
class CustomerDetail:
    def __init__(self):
        self.required_params = ["id", "trackId", "groupIds", "host", "port"]

    @ValidateParams(lambda self: self.required_params)
    def run(self, *args, **kwargs):
        mongo_client = MongoDB()
        mongo_client.setup_db(
            username=kwargs["username"],
            password=kwargs["password"],
            host=kwargs["host"],
            port=kwargs["port"],
            auth=kwargs["auth"]
        )
        
        # Query groups
        group_list = []
        for group_id in kwargs.get("groupIds", []):
            group_query = mongo_client.find(
                db_name= kwargs["db"],
                col_name= "cam_groups",
                query= {
                    "group_id": group_id
                }
            )["result"]
            group_list.extend(list(group_query))

        # Query cameras
        cameras_group_lists = []
        for group_id in kwargs.get("groupIds", []):
            cameras_query = mongo_client.find(
                db_name=kwargs["db"],
                col_name="cameras",
                query={
                    "group_id": group_id
                }
            )["result"]
            cameras_list = list(cameras_query)
            for group in group_list:
                if group.get("group_id") == group_id:
                    group_name = group.get("name")
                    cameras_group_lists.append({
                        "group_id": group_id,
                        "group_name": group_name,
                        "cameras": cameras_list
                    })
                    break
        result = {
            "visitCount": 0,
            "averageVisit": "0 minutes",
            "lastVisitDays": -1,
            "dates": []
        }

        # Query events
        for cameras_group_list in cameras_group_lists:
            cameras_list = cameras_group_list.get('cameras')
            camera_ids = [camera.get("camera_id") for camera in cameras_list]
            face_events_query = mongo_client.find(
                db_name=kwargs["db"],
                col_name="face_events",
                query={
                    "camera_id": {"$in": camera_ids},
                    "timestamp": {
                        "$gte": datetime.datetime.fromisoformat(kwargs.get("visitDateFrom")),
                        "$lte": datetime.datetime.fromisoformat(kwargs.get("visitDateTo"))
                    }
                }
            )['result']
            face_events_query = list(face_events_query)

            for face_event in face_events_query:
                face_event["timestamp"] = face_event.get("timestamp").strftime("%Y-%m-%d %H:%M:%S")

            result["visitCount"] += len(face_events_query)
            result["dates"].append({
                "groupId": cameras_group_list.get('group_id'),
                "groupName": cameras_group_list.get('group_name'),
                'events': face_events_query
            })

        # Query face identity
        face_identity_query = mongo_client.find(
            db_name=kwargs["db"],
            col_name="face_identities",
            query={
                "face_id": kwargs.get("id")
            }
        )['result']

        face_identity_query = list(face_identity_query)

        if face_identity_query:
            last_seen = face_identity_query[0].get("last_seen")
            result["lastVisitDays"] = (datetime.datetime.now() - last_seen).days

        return result
    
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
    serialize_class = ClassSerializer.serialize(CustomerDetail)
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
        "id": "F-2",
        "trackId": "T-1",
        "groupIds": ["CG-1", "CG-2"],
        "visitDateFrom": "2021-01-01T00:00:00.000Z",
        "visitDateTo": "2025-12-31T23:59:59.999Z"
    }

    res = report.run(**params)
    print(res)
            
