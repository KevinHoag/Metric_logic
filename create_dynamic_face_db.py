from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType, utility

# Connect to Milvus
connections.connect(host='172.21.5.197', port='19530')

# Define the fields with correct data types
fields = [
    # Primary key must be INT64 in Milvus, can't be STRING
    FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
    
    # Vector embedding field
    FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=512),
    
    # Core fields
    FieldSchema(name="cam_id", dtype=DataType.VARCHAR, max_length=200),
    FieldSchema(name="event_time", dtype=DataType.INT64),
    FieldSchema(name="img_path", dtype=DataType.VARCHAR, max_length=200),
    FieldSchema(name="bbox", dtype=DataType.ARRAY, element_type=DataType.FLOAT, max_capacity=4),
    FieldSchema(name="confidence", dtype=DataType.FLOAT),
    
    # Additional fields with correct types
    FieldSchema(name="label", dtype=DataType.VARCHAR, max_length=200),
    FieldSchema(name="check_iqa", dtype=DataType.BOOL),
    FieldSchema(name="track_id", dtype=DataType.INT64),  # Changed from INT16 to INT64 as Milvus doesn't support INT16
    
    # Changed STRING to VARCHAR with max_length for landmark and location
    FieldSchema(name="landmark", dtype=DataType.VARCHAR, max_length=1024),  # Increased length for landmark data
    FieldSchema(name="location", dtype=DataType.VARCHAR, max_length=200)
]

# Create schema with dynamic fields enabled
schema = CollectionSchema(
    fields,
    "Face events collection with dynamic fields",
    enable_dynamic_field=True
)

# Create collection
collection_name = "smh_face_events"

# Drop if exists
if utility.has_collection(collection_name):
    utility.drop_collection(collection_name)

collection = Collection(collection_name, schema)

# Create index on vector field
index_params = {
    "index_type": "AUTOINDEX",
    "metric_type": "L2",
    "params": {}
}

collection.create_index(
    field_name="embedding",
    index_params=index_params
)

# Load the collection
collection.load()

# Example insertion with all fields
def insert_face_data(
    collection: Collection,
    embedding: list,
    cam_id: str,
    event_time: int,
    img_path: str,
    bbox: list,
    confidence: float,
    label: str,
    check_iqa: bool,
    track_id: int,
    landmark: str,
    location: str,
    **dynamic_fields
):
    """
    Insert face data with all fields and support for additional dynamic fields
    """
    data = {
        "embedding": embedding,
        "cam_id": cam_id,
        "event_time": event_time,
        "img_path": img_path,
        "bbox": bbox,
        "confidence": confidence,
        "label": label,
        "check_iqa": check_iqa,
        "track_id": track_id,
        "landmark": landmark,
        "location": location
    }
    
    # Add any additional dynamic fields
    data.update(dynamic_fields)
    
    # Insert the data
    collection.insert([data])

# Example usage
if __name__ == "__main__":
    import numpy as np
    import time
    # Example data
    sample_data = {
        "embedding": np.random.rand(512).tolist(),
        "cam_id": "CAM_001",
        "event_time": int(time.time()),
        "img_path": "/path/to/face.jpg",
        "bbox": [100.0, 200.0, 300.0, 400.0],
        "confidence": 0.95,
        "label": "person_1",
        "check_iqa": True,
        "track_id": 12345,
        "landmark": "[[100,200], [150,250], [200,300]]",  # Landmark as string JSON
        "location": "entrance",
        # Dynamic field example
        "temperature": 36.5
    }
    
    # Insert data
    collection.insert([sample_data])