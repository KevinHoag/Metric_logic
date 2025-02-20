from pymongo import MongoClient, ASCENDING, DESCENDING
from base import MongoConfig, MongoDB
import datetime

def function(db:MongoDB, **params):
    db = db.db['distill_db']
    params = params.get('params')

    # Query groups
    group_query = db.cam_groups.find(
        {
            "group_id": {
                "$in": params.get("groupIds", [])
            }
        }
    )
    group_list = list(group_query)

    # Query cameras
    camera_group_lists = [
        {"camera": [camera for camera in db.cameras.find({"group_id": {"$in": [group_id]}})], "group_id": group_id}
        for group_id in params.get("groupIds", [])
    ]

    face_events_list = []
    for camera_group_list in camera_group_lists:
        camera_list = camera_group_list.get("camera")
        camera_ids = [camera.get("camera_id") for camera in camera_list]
        face_events_query = db.face_events.aggregate([
            {
                "$match": {
                    "camera_id": {"$in": camera_ids},
                    "timestamp": {
                        "$gte": datetime.datetime.fromisoformat(params.get("visitDateFrom")),
                        "$lte": datetime.datetime.fromisoformat(params.get("visitDateTo"))
                    }
                }
            },
            {
                "$group": {
                    "_id": "$face_id",
                    "visit_count": {"$sum": 1},
                    "group_id": {"$first": camera_group_list.get("group_id")}
                }
            }
        ])

        face_events_query = list(face_events_query)
        face_events_list.extend(daily_stat for daily_stat in face_events_query)
    face_events_list.sort(key=lambda x: x.get(params.get("sortBy")), reverse=params.get("order") == "desc")
    face_events_list = face_events_list[0:int(params.get("pageSize")) * int(params.get("page"))]

    face_id_lists = [daily_stat.get("_id") for daily_stat in face_events_list]
    face_identities_query = db.face_identities.find(
        {
            "face_id": {
                "$in": face_id_lists
            }
        }
    )
    face_identities_list = list(face_identities_query)

    for face_identity in face_identities_list:
        face_events_query = db.face_events.find(
            {
                "face_id": face_identity.get("face_id"),
                "timestamp": face_identity.get("last_seen")
            }
        )
        face_events_query = list(face_events_query)[0]
        face_identity['track_id'] = face_events_query.get('track_id')
        face_identity['event_id'] = face_events_query.get('event_id')

    kq = face_events_list
    for item in kq:
        for group in group_list:
            if item.get("group_id") == group.get("group_id"):
                item["groupName"] = group.get("name")
                break
        for face_identity in face_identities_list:
            if item.get("_id") == face_identity.get("face_id"):
                item["trackId"] = face_identity.get("track_id")
                item["eventId"] = face_identity.get("event_id")
                item['label'] = face_identity.get('labels')
                item['lastVisitDay'] = (datetime.datetime.now() - face_identity.get('last_seen')).days
                item['lastVisitTime'] = face_identity.get('last_seen').strftime("%Y-%m-%d %H:%M:%S")
                item["fullName"] = face_identity.get("username")
                break

    return kq

if __name__ == "__main__":
    config = MongoConfig("localhost", 27017, "distill_db", "", "")
    db = MongoDB(config)

    params = {
        "page": 1,
        "pageSize": 2,
        "search": "",
        "status": "identified",
        "sortBy": "visit_count",
        "order": "desc",
        "groupIds": ["CG-5", "CG-2"],
        "visitDateFrom": "2025-02-16T00:00:00.000Z",
        "visitDateTo": "2025-02-17T23:59:59.999Z",
        "tags": ["vip", "regular"]
    }
    
    print(function(db, params=params))