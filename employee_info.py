from pymongo import MongoClient, ASCENDING, DESCENDING
from base import MongoConfig, MongoDB
import datetime

def function(db:MongoDB, **params):
    db = db.db['distill_db']
    params = params.get('params')

    # Query face
    face_query = db.face_identities.find(
        {
            "face_id": params.get("face_id")
        },
        {
            "face_id": 1,
            "username": 1,
            'metadata': 1,
            "labels": 1
        }
    )
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

if __name__ == "__main__":
    config = MongoConfig("localhost", 28000, "distill_db", "", "")
    db = MongoDB(config)

    result = function(db, params={"face_id": "F-1"})
    print(result)