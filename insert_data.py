from datetime import datetime, timedelta
from random import randint, choice
from pymongo import MongoClient
from faker import Faker

client = MongoClient("mongodb://localhost:27017/")
db = client["distill_db"]

# Define time range
start_time = datetime(2025, 2, 16, 0, 0, 0)  # Feb 16, 2025, 00:00:00
end_time = datetime(2025, 2, 17, 23, 59, 59)  # Feb 17, 2025, 23:59:59

def random_datetime():
    """Generate a random datetime between start_time and end_time."""
    return start_time + timedelta(seconds=randint(0, int((end_time - start_time).total_seconds())))

# Sample data
cam_groups = [
    {"group_id": f"CG-{i}", "name": f"Group {i}", "location": f"Location {i}", "created_at": random_datetime()} 
    for i in range(1, 6)
]

cameras = [
    {"camera_id": f"CAM-{i}", "group_id": choice(cam_groups)["group_id"], "location": f"Area {i}", 
     "created_at": random_datetime(), "last_event": random_datetime(), 
     "status": choice(["active", "inactive"])} 
    for i in range(1, 11)
]

# Temporary storage to assign correct `last_seen`
face_events_dict = {}

face_events = []
for i in range(1, 51):
    event_time = random_datetime()
    face_id = choice([f"F-{i}" for i in range(1, 21)])  # Pick a random face_id
    
    # Track min/max timestamps per face_id
    if face_id not in face_events_dict:
        face_events_dict[face_id] = {"first_seen": event_time, "last_seen": event_time}
    else:
        face_events_dict[face_id]["first_seen"] = min(face_events_dict[face_id]["first_seen"], event_time)
        face_events_dict[face_id]["last_seen"] = max(face_events_dict[face_id]["last_seen"], event_time)
    
    face_events.append({
        "event_id": f"E-{i}",
        "milvus_id": f"M-{i}",
        "face_id": face_id,
        "camera_id": choice(cameras)["camera_id"],
        "timestamp": event_time,
        "confidence": round(randint(80, 99) + randint(0, 99)/100, 2),
        "track_id": f"T-{i}"
    })

fake = Faker()

# Sample metadata template
def generate_metadata():
    return {
        "phoneNumber": fake.phone_number(),
        "email": fake.email(),
        "position": choice(["Store Manager", "Security", "Cashier", "Sales Associate"]),
        "department": choice(["Sales", "Security", "Customer Service"]),
        "joinDate": fake.date_between(start_date="-10y", end_date="today").strftime("%Y-%m-%d"),
        "groupId": fake.uuid4()[:8],  # Simulating a unique group ID
        "groupName": choice(["Downtown Branch", "Uptown Mall", "Central Store"]),
        "status": choice(["active", "inactive"]),
        "avatar": fake.image_url(),
        "address": fake.address(),
        "notes": fake.sentence()
    }

# Generate face identities with corrected `first_seen` and `last_seen`
face_identities = [
    {
        "face_id": face_id,
        "username": f"user_{i}",
        "first_seen": face_events_dict[face_id]["first_seen"],
        "last_seen": face_events_dict[face_id]["last_seen"],
        "total_visits": randint(1, 20),
        "labels": [choice(["VIP", "staff", "visitor"])],
        "metadata": generate_metadata()  # Adding metadata field
    }
    for i, face_id in enumerate(face_events_dict.keys(), start=1)
]

daily_stats = [
    {
        "stat_id": f"DS-{i}",
        "date": random_datetime(),
        "camera_id": choice(cameras)["camera_id"],
        "face_id": choice(face_identities)["face_id"],
        "visit_count": randint(1, 10),
        "first_event": random_datetime(),
        "last_event": random_datetime()
    }
    for i in range(1, 16)
]

# Insert data into MongoDB
db["cam_groups"].insert_many(cam_groups)
db["cameras"].insert_many(cameras)
db["face_identities"].insert_many(face_identities)
db["face_events"].insert_many(face_events)
db["daily_stats"].insert_many(daily_stats)

print("Fake data inserted successfully within the time range 16-17 Feb 2025!")
