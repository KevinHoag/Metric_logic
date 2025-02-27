from datetime import datetime, timedelta, timezone
from functools import wraps
import pytz
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Any, Callable, Type, get_type_hints
from db import MongoDB
import inspect

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

class CustomerCountMetric:
    def __init__(self):
        self.required_params = ["param_startTime", "param_dueTime", "param_groupIds", "host", "port"]

    def generate_time_blocks(
            self, 
            start_time: datetime, 
            due_time: datetime, 
            base_time: str
        ) -> List[Dict[str, datetime]]:
        """
        Create Time block flow by base_time
        
        Parameters:
        - start_time
        - due_time
        - base_time: Time units (hourly, daily, weekly, monthly, yearly)
        
        Returns:
        - List timw blocks
        """
        blocks = []
        current = start_time
        
        while current < due_time:
            from_time = current
            
            if base_time == 'hourly':
                to_time = current + timedelta(hours=1)
            elif base_time == 'daily':
                to_time = current + timedelta(days=1)
            elif base_time == 'weekly':
                to_time = current + timedelta(weeks=1)
            elif base_time == 'monthly':
                to_time = current + relativedelta(months=1)
            elif base_time == 'yearly':
                to_time = current + relativedelta(years=1)
            else:
                raise ValueError(f"Invalid base_time: {base_time}")
            
            if to_time > due_time:
                to_time = due_time
            
            blocks.append({
                "from": from_time,
                "to": to_time
            })
            
            current = to_time
        
        return blocks

    def ensure_timezone(self, dt, default_tz=timezone.utc):
        """Đảm bảo datetime có thông tin múi giờ"""
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=default_tz)
        return dt
    
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

        # Validate base_time
        valid_base_times = ['hourly', 'daily', 'weekly', 'monthly', 'yearly']
        if kwargs["param_baseTime"] not in valid_base_times:
            raise ValueError(f"Invalid base_time: {kwargs["param_baseTime"]}. Must be one of {valid_base_times}")
        
        start_datetime = datetime.fromisoformat(kwargs["param_startTime"].replace('Z', '+00:00'))
        due_datetime = datetime.fromisoformat(kwargs["param_dueTime"].replace('Z', '+00:00'))
        
        time_blocks = self.generate_time_blocks(start_datetime, due_datetime, kwargs["param_baseTime"])

        camera_list = mongo_client.find(
            db_name=kwargs["db"],
            col_name="cameras",
            query={
                "group_id": {"$in": kwargs["param_groupIds"]}
            }
        )["result"]
        camera_list = list(camera_list)
        camera_ids = list(camera_list)
        camera_ids = [camera["camera_id"] for camera in camera_ids]

        face_events = mongo_client.find(
            db_name=kwargs["db"],
            col_name="face_events",
            query={
                "camera_id": {"$in": camera_ids},
                "timestamp": {"$gte": start_datetime, "$lte": due_datetime}
            }
        )["result"]
        
        if len(face_events) == 0:
            return {
                "results": [],
                "metadata": {
                    "total_count": 0,
                    "total_new_customer": 0,
                    "last_updated": datetime.now(pytz.UTC).isoformat(),
                    "base_time": kwargs["param_baseTime"]
                }
            }
        
        unique_face_ids = list(set(event["face_id"] for event in face_events))

        face_identities = mongo_client.find(
            db_name=kwargs["db"],
            col_name="face_identities",
            query={
                "face_id": {"$in": unique_face_ids}
            }
        )['result']
        
        face_first_seen_map = {face["face_id"]: face["first_seen"] for face in face_identities}
        
        results = []
        total_count = 0
        total_new_customer = 0
        
        for block in time_blocks:
            block_events = [
                event for event in face_events
                if block["from"] <= self.ensure_timezone(event["timestamp"]) < block["to"]
            ]
            
            block_customers = list(set(event["face_id"] for event in block_events))
            block_count = len(block_customers)
            
            new_customers = 0
            old_customers = 0
            
            for face_id in block_customers:
                first_seen = self.ensure_timezone(face_first_seen_map.get(face_id))
                if first_seen is None:
                    continue
                
                if block["from"] <= first_seen < block["to"]:
                    new_customers += 1
                elif first_seen < block["from"]:
                    old_customers += 1
            
            block_result = {
                "time_range": {
                    "from": block["from"].isoformat(),
                    "to": block["to"].isoformat(),
                },
                "count": block_count,
                "new_customer": new_customers,
                "old_customer": old_customers
            }
            
            results.append(block_result)
            total_count += block_count
            total_new_customer += new_customers
        
        return {
            "results": results,
            "metadata": {
                "total_count": total_count,
                "total_new_customer": total_new_customer,
                "last_updated": datetime.now(pytz.UTC).isoformat(),
                "base_time": kwargs["param_baseTime"]
            }
        }

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
    serialize_class = ClassSerializer.serialize(CustomerCountMetric)
    # print("Serialized Data:\n", serialize_class)

    # Deserialize the class definition
    deserialized_class = ClassSerializer.deserialize(serialize_class)
    # print("Deserialized Class:\n", inspect.getsource(deserialized_class))

    # Use the deserialized class
    report = deserialized_class()
    res = report.run(
        username="",
        password="",
        # host="172.21.5.197",
        host="localhost",
        port=28000,
        db="distill_db",
        auth=None,
        param_baseTime = 'daily',
        param_groupIds = ["CG-1"],
        param_startTime = "2024-02-15T23:59:59.999Z",
        param_dueTime = "2025-02-21T23:59:59.999Z"
    )
    print(res)