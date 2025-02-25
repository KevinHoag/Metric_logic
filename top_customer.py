from functools import wraps
from typing import Dict, List, Any, Callable, Type, get_type_hints
import datetime
import inspect
import pytz
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


class TopCustomerMetric:
    """
    Top khách hàng thân thiết
    """
    
    def __init__(self):
        """Initialize required parameters"""
        self.required_params = [
            "params_startTime", 
            "params_dueTime", 
            "params_cameraIds", 
            "params_baseTime",
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
    
    @ValidateParams(lambda self: self.required_params)
    def run(self, *args, **kwargs):
        """Run the top customer metric calculation"""
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
        if kwargs["params_baseTime"] not in valid_base_times:
            raise ValueError(f"Invalid base_time: {kwargs['params_baseTime']}. Must be one of {valid_base_times}")
        
        # Validate limit parameter
        limit = int(kwargs.get("params_limit", 5))
        if limit <= 0:
            raise ValueError("Limit must be a positive number")
        
        # Convert string timestamps to datetime objects
        start_datetime = self.ensure_timezone(
            datetime.datetime.fromisoformat(kwargs["params_startTime"].replace('Z', '+00:00'))
        )
        due_datetime = self.ensure_timezone(
            datetime.datetime.fromisoformat(kwargs["params_dueTime"].replace('Z', '+00:00'))
        )
        
        # Parse camera IDs
        camera_ids = kwargs["params_cameraIds"]
        if isinstance(camera_ids, str):
            camera_ids = camera_ids.split(',')
        
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
                    "current_month": start_datetime.replace(day=1).isoformat(),
                    "last_updated": datetime.datetime.now(pytz.UTC).isoformat(),
                    "has_more": False,
                    "limit": limit
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
        
        # Create mapping for quick lookup
        face_id_to_identity = {face["face_id"]: face for face in face_identities}
        
        # Compute visit statistics for each face ID
        customer_stats = {}
        for event in face_events:
            face_id = event["face_id"]
            timestamp = event["timestamp"]
            
            if face_id not in customer_stats:
                customer_stats[face_id] = {
                    "visit_count": 0, 
                    "visit_days": set(), 
                    "last_visit": None
                }
            
            # Update stats
            customer_stats[face_id]["visit_count"] += 1
            customer_stats[face_id]["visit_days"].add(timestamp.date())
            
            # Update last visit
            last_visit = customer_stats[face_id]["last_visit"]
            if last_visit is None or timestamp > last_visit:
                customer_stats[face_id]["last_visit"] = timestamp
        
        # Sort customers by visit count (descending)
        sorted_customers = sorted(
            customer_stats.items(),
            key=lambda x: (x[1]["visit_count"], len(x[1]["visit_days"])),
            reverse=True
        )
        
        # Limit number of customers returned
        top_customers = sorted_customers[:limit]
        has_more = len(sorted_customers) > limit
        
        # Format results
        results = []
        for face_id, stats in top_customers:
            # Get customer info from face_identities
            identity = face_id_to_identity.get(face_id, {})
            
            # Create customer info object
            customer_info = {
                "user_id": face_id,
                "name": identity.get("username", "Unknown"),
                "age": identity.get("metadata", {}).get("age", 30),
                "gender": identity.get("metadata", {}).get("gender", 0),
                "visits": {
                    "count": stats["visit_count"],
                    "days": len(stats["visit_days"])
                },
                "last_visit": stats["last_visit"].isoformat() if stats["last_visit"] else None
            }
            
            results.append(customer_info)
        
        # Create metadata
        current_month = start_datetime.replace(day=1)
        metadata = {
            "current_month": current_month.isoformat(),
            "last_updated": datetime.datetime.now(pytz.UTC).isoformat(),
            "has_more": has_more,
            "limit": limit
        }
        
        return {
            "results": results,
            "metadata": metadata
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
    # parser = argparse.ArgumentParser(description='Top Customer Metric')
    # parser.add_argument('--start-time', required=True, help='Start time in ISO format')
    # parser.add_argument('--due-time', required=True, help='Due time in ISO format')
    # parser.add_argument('--camera-ids', required=True, help='Comma-separated list of camera IDs')
    # parser.add_argument('--base-time', required=True, 
    #                     choices=['hourly', 'daily', 'weekly', 'monthly', 'yearly'],
    #                     help='Time block unit')
    # parser.add_argument('--limit', type=int, default=5, help='Limit number of customers')
    # parser.add_argument('--host', default='localhost', help='MongoDB host')
    # parser.add_argument('--port', type=int, default=27017, help='MongoDB port')
    # parser.add_argument('--username', default='', help='MongoDB username')
    # parser.add_argument('--password', default='', help='MongoDB password')
    # parser.add_argument('--db', default='distill_db', help='MongoDB database name')
    # parser.add_argument('--auth', default='admin', help='MongoDB auth database')
    # parser.add_argument('--output', help='Output file path (JSON)')
    
    # args = parser.parse_args()
    
    try:
        # Example usage
        serialize_class = ClassSerializer.serialize(TopCustomerMetric)
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
            "params_startTime": "2021-02-17T23:59:59.999Z",
            "params_dueTime": "2025-02-17T23:59:59.999Z",
            "params_cameraIds": ["CAM-5", "CAM-2", "CAM-1"],
            "params_baseTime": "hourly",
            "params_limit": 4
        }
        
        result = metric.run(**params)
        print(result)
        
        # if args.output:
        #     with open(args.output, 'w') as f:
        #         json.dump(result, f, indent=2, default=str)
        #     print(f"Results saved to {args.output}")
        # else:
        #     print(json.dumps(result, indent=2, default=str))
            
    except Exception as e:
        print(f"Error: {e}")