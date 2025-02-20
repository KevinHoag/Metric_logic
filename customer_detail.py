from pymongo import MongoClient, ASCENDING, DESCENDING
from base import MongoConfig, MongoDB
import datetime

def function(db:MongoDB, **params):
    db = db.db['distill_db']
    params = params.get('params')

    # Query groups
    group_list = []
    for group_id in params.get("groupIds", []):
        group_query = db.cam_groups.find(
            {
                "group_id": group_id
            }
        )
        group_list.extend(list(group_query))

    # Query cameras
    cameras_group_lists = []
    for group_id in params.get("groupIds", []):
        cameras_query = db.cameras.find({"group_id": group_id})
        cameras_list = list(cameras_query)
        for group in group_list:
            if group.get("group_id") == group_id:
                group_name = group.get("name")
                break
        cameras_group_lists.append({
            "group_id": group_id,
            "group_name": group_name,
            "cameras": cameras_list
        })

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
        face_events_query = db.face_events.find(
            {
                "camera_id": {"$in": camera_ids},
                "timestamp": {
                    "$gte": datetime.datetime.fromisoformat(params.get("visitDateFrom")),
                    "$lte": datetime.datetime.fromisoformat(params.get("visitDateTo"))
                }
            },
            {
                "_id": 0,
                "event_id": 1,
                "camera_id": 1,
                "timestamp": 1
            }
        )

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
    face_identity_query = db.face_identities.find(
        {
            "face_id": params.get("id")
        },
        {
            "last_seen": 1
        }
    )

    face_identity_query = list(face_identity_query)


    if face_identity_query:
        last_seen = face_identity_query[0].get("last_seen")
        result["lastVisitDays"] = (datetime.datetime.now() - last_seen).days

    return result

if __name__ == "__main__":
    config = MongoConfig("localhost", 28000, "distill_db", "", "")
    db = MongoDB(config)

    result = function(db, params={"id": "F-1", "groupIds": ["CG-1", "CG-2"], "visitDateFrom": "2025-02-16T00:00:00", "visitDateTo": "2025-02-17T23:59:59"})
    print(result)