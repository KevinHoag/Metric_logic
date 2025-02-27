from functools import wraps
from typing import Dict, List, Any, Callable, Type, get_type_hints
import datetime
import inspect
import pytz
from dateutil.relativedelta import relativedelta
import json
import argparse
from db import MongoDB

class ValidateParams:
    def __init__(self, required_params_getter: Callable):
        self.required_params_getter = required_params_getter

    def validate_type(self, value: Any, expected_type: Type) -> bool:
        if expected_type == datetime.datetime and isinstance(value, str):
            try:
                datetime.datetime.fromisoformat(value)
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


class EmployeeCountMetric:
    """
    Thống kê số lượng nhân viên theo khoảng thời gian
    """
    
    def __init__(self):
        """Initialize required parameters"""
        self.required_params = [
            "param_startTime", 
            "param_dueTime", 
            "param_cameraIds", 
            "param_baseTime",
            "host", 
            "port"
        ]
        
    def ensure_timezone(self, dt, default_tz=pytz.UTC):
        """Ensure datetime has timezone information"""
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=default_tz)
        return dt
    
    def generate_time_blocks(self, start_time: datetime.datetime, due_time: datetime.datetime, base_time: str) -> List[Dict[str, datetime.datetime]]:
        """
        Create time blocks based on base_time
        
        Parameters:
        - start_time: Start time
        - due_time: End time
        - base_time: Time unit (hourly, daily, weekly, monthly, yearly)
        
        Returns:
        - List of time blocks
        """
        blocks = []
        current = start_time
        
        while current < due_time:
            from_time = current
            
            # Calculate block end time based on base_time
            if base_time == 'hourly':
                to_time = current + datetime.timedelta(hours=1)
            elif base_time == 'daily':
                to_time = current + datetime.timedelta(days=1)
            elif base_time == 'weekly':
                to_time = current + datetime.timedelta(weeks=1)
            elif base_time == 'monthly':
                to_time = current + relativedelta(months=1)
            elif base_time == 'yearly':
                to_time = current + relativedelta(years=1)
            else:
                raise ValueError(f"Invalid base_time: {base_time}")
            
            # If block end time exceeds due_time, set it to due_time
            if to_time > due_time:
                to_time = due_time
            
            blocks.append({
                "from": from_time,
                "to": to_time
            })
            
            # Update current time for next block
            current = to_time
        
        return blocks
    
    @ValidateParams(lambda self: self.required_params)
    def run(self, *args, **kwargs):
        """Run the employee count metric calculation"""
        # Connect to MongoDB
        mongo_client = MongoDB()
        mongo_client.setup_db(
            username=kwargs.get("username", ""),
            password=kwargs.get("password", ""),
            host=kwargs["host"],
            port=kwargs["port"],
            auth=kwargs.get("auth", "admin"),
        )
        
        # Validate base_time parameter
        valid_base_times = ['hourly', 'daily', 'weekly', 'monthly', 'yearly']
        base_time = kwargs["param_baseTime"]
        if base_time not in valid_base_times:
            raise ValueError(f"Invalid base_time: {base_time}. Must be one of {valid_base_times}")
        
        # Convert string timestamps to datetime objects
        start_datetime = self.ensure_timezone(
            datetime.datetime.fromisoformat(kwargs["param_startTime"].replace('Z', '+00:00'))
        )
        due_datetime = self.ensure_timezone(
            datetime.datetime.fromisoformat(kwargs["param_dueTime"].replace('Z', '+00:00'))
        )
        
        # Parse camera IDs
        camera_ids = mongo_client.find(
            db_name=kwargs["db"],
            col_name="cameras",
            query={
                "group_id": {"$in": kwargs['param_groupIds']}
            }
        )["result"]
        camera_ids = list(camera_ids)
        camera_ids = [camera["camera_id"] for camera in camera_ids]
        
        # Generate time blocks
        time_blocks = self.generate_time_blocks(start_datetime, due_datetime, base_time)
        
        # Get face events within the time range
        face_events = mongo_client.find(
            db_name=kwargs["db"],
            col_name="face_events",
            query={
                "camera_id": {"$in": camera_ids},
                "timestamp": {"$gte": start_datetime, "$lte": due_datetime}
            }
        )["result"]
        
        # If no events, return empty result
        if len(face_events) == 0:
            return {
                "results": [],
                "metadata": {
                    "total_count": 0,
                    "last_updated": datetime.datetime.now(pytz.UTC).isoformat(),
                    "base_time": base_time
                }
            }
        
        # Ensure all timestamps have timezone info
        for event in face_events:
            event["timestamp"] = self.ensure_timezone(event["timestamp"])
        
        # Get unique face IDs from events
        unique_face_ids = list(set(event["face_id"] for event in face_events))
        
        # Get face identities for these face IDs
        face_identities = mongo_client.find(
            db_name=kwargs["db"],
            col_name="face_identities",
            query={
                "face_id": {"$in": unique_face_ids}
            }
        )["result"]
        
        # Filter out only face_identities with "staff" label
        employee_face_ids = []
        for face in face_identities:
            labels = face.get("labels", [])
            if "staff" in labels:
                employee_face_ids.append(face["face_id"])
        
        # If no staff members, return empty result
        if len(employee_face_ids) == 0:
            return {
                "results": [],
                "metadata": {
                    "total_count": 0,
                    "last_updated": datetime.datetime.now(pytz.UTC).isoformat(),
                    "base_time": base_time
                }
            }
        
        # Filter only events from staff members
        employee_events = [event for event in face_events if event["face_id"] in employee_face_ids]
        
        # Create mapping from face_id to first_seen to identify new employees
        face_id_to_first_seen = {}
        for face in face_identities:
            if face["face_id"] in employee_face_ids:
                first_seen = self.ensure_timezone(face["first_seen"])
                face_id_to_first_seen[face["face_id"]] = first_seen
        
        # Process data for each time block
        results = []
        all_employees = set()  # Set of all employees that appeared
        
        for block in time_blocks:
            # Filter events in current time block
            block_events = []
            for event in employee_events:
                event_timestamp = event["timestamp"]
                if block["from"] <= event_timestamp < block["to"]:
                    block_events.append(event)
            
            # If no events in block, add block with zero values
            if not block_events:
                block_result = {
                    "time_range": {
                        "from": block["from"].isoformat(),
                        "to": block["to"].isoformat()
                    },
                    "count": 0,
                    "new_appear_employees": 0
                }
                results.append(block_result)
                continue
            
            # Get unique employees in this block
            block_employees = set(event["face_id"] for event in block_events)
            
            # Count new employees appearing in this block
            new_employees = 0
            for face_id in block_employees:
                first_seen = face_id_to_first_seen.get(face_id)
                if first_seen and block["from"] <= first_seen < block["to"]:
                    new_employees += 1
            
            # Create result for this time block
            block_result = {
                "time_range": {
                    "from": block["from"].isoformat(),
                    "to": block["to"].isoformat()
                },
                "count": len(block_employees),
                "new_appear_employees": new_employees
            }
            
            results.append(block_result)
            all_employees.update(block_employees)
        
        # Return results and metadata
        return {
            "results": results,
            "metadata": {
                "total_count": len(all_employees),
                "last_updated": datetime.datetime.now(pytz.UTC).isoformat(),
                "base_time": base_time
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
    serialize_class = ClassSerializer.serialize(EmployeeCountMetric)
    # print("Serialized Data:\n", serialize_class)

    # Deserialize the class definition
    deserialized_class = ClassSerializer.deserialize(serialize_class)
    # print("Deserialized Class:\n", inspect.getsource(deserialized_class))

    # Use the deserialized class
    metric = deserialized_class()
    
    params = {
        "username": "",
        "password": "",
        "host": "localhost",
        "port": 28000,
        "db": "distill_db",
        "auth": "admin",
        "param_startTime": "2025-02-16T23:59:59.999Z",
        "param_dueTime": "2025-02-17T23:59:59.999Z",
        "param_cameraIds": ["CAM-5", "CAM-2", "CAM-1", "CAM-3", "CAM-4"],
        "param_baseTime": "hourly",
    }
    
    result = metric.run(
        username="",
        password="",
        host="locahost",
        port=28000,
        db="distill_db",
        auth=None,
        param_baseTime = 'daily',
        param_groupIds = ["CG-5", "CG-2"],
        param_startTime = "2025-02-15T23:59:59.999Z",
        param_dueTime = "2025-02-21T23:59:59.999Z"
    )
    print(result)
        
