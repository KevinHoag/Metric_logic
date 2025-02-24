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
    
class EmployeeInfo:
    def __init__(self):
        self.required_params = ["employee_id", "host", "port"]

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
        
        # Insert data into MongoDB
        face_query = mongo_client.find(
            db_name=kwargs["db"],
            col_name="face_identities",
            query={"face_id": kwargs["face_id"]},
        )["result"]
        face_list = list(face_query)

        result = {}
        if len(face_list) > 0:
            face = face_list[0]
            metadata = face.get("metadata")
            result = {
                "face_id": face.get("face_id"),
                "username": face.get("username"),
                "tags": face.get("labels")
            }
            result.update(metadata)
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
    serialize_class = ClassSerializer.serialize(EmployeeInfo)
    print("Serialized Data:\n", serialize_class)

    # Deserialize the class definition
    deserialized_class = ClassSerializer.deserialize(serialize_class)
    print("Deserialized Class:\n", inspect.getsource(deserialized_class))

    # Use the deserialized class
    report = deserialized_class()
    res = report.run(
        username="mongoadmin",
        password="mongoadmin",
        host="localhost",
        port="27017",
        db="distill_db",
        auth="admin",
        id="emp123"
    )
    print(res)