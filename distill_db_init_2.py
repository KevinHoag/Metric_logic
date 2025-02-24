# src/db/mongo/distill_db_init.py

import logging
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import CollectionInvalid
from datetime import datetime
from base import MongoConfig, MongoDB

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def init_cam_groups_collection(db: MongoDB):
    """Initialize store groups collection"""
    # collection = db.db.cam_groups
    collection = db.db['distill_db']['cam_groups']
    
    # Create indexes
    collection.create_index([("group_id", ASCENDING)], unique=True)
    collection.create_index([("name", ASCENDING)])
    collection.create_index([("location", ASCENDING)])
    collection.create_index([("created_at", DESCENDING)])
    
    logger.info("Store groups collection initialized")

def init_cameras_collection(db: MongoDB):
    """Initialize cameras collection"""
    # collection = db.db.cameras
    collection = db.db['distill_db']['cameras']
    
    
    # Create indexes
    collection.create_index([("camera_id", ASCENDING)], unique=True)
    collection.create_index([("group_id", ASCENDING)])
    collection.create_index([("location", ASCENDING)])
    collection.create_index([("created_at", DESCENDING)])
    collection.create_index([("last_event", DESCENDING)])
    collection.create_index([("status", ASCENDING)])
    
    logger.info("Cameras collection initialized")

def init_face_identities_collection(db: MongoDB):
    """Initialize face identities collection"""
    # collection = db.db.face_identities
    collection = db.db['distill_db']['face_identities']
    
    # Create indexes
    collection.create_index([("face_id", ASCENDING)], unique=True)
    collection.create_index([("username", ASCENDING)])
    collection.create_index([("first_seen", DESCENDING)])
    collection.create_index([("last_seen", DESCENDING)])
    collection.create_index([("total_visits", DESCENDING)])
    collection.create_index([("labels", ASCENDING)])
    collection.create_index([("metadata", ASCENDING)])
    
    logger.info("Face identities collection initialized")

def init_face_events_collection(db: MongoDB):
    """Initialize face events collection"""
    # collection = db.db.face_events
    collection = db.db['distill_db']['face_events']
    
    # Create indexes
    collection.create_index([("event_id", ASCENDING)], unique=True)
    collection.create_index([("milvus_id", ASCENDING)])
    collection.create_index([("face_id", ASCENDING)])
    collection.create_index([("camera_id", ASCENDING)])
    collection.create_index([("timestamp", DESCENDING)])
    collection.create_index([("confidence", DESCENDING)])
    collection.create_index([("track_id", ASCENDING)])
    
    logger.info("Face events collection initialized")

def init_daily_stats_collection(db: MongoDB):
    """Initialize daily stats collection"""
    # collection = db.db.daily_stats
    collection = db.db['distill_db']['daily_stats']
    
    # Create indexes
    collection.create_index([("stat_id", ASCENDING)], unique=True)
    collection.create_index([("date", DESCENDING)])
    collection.create_index([("camera_id", ASCENDING)])
    collection.create_index([("face_id", ASCENDING)])
    collection.create_index([("visit_count", DESCENDING)])
    collection.create_index([("first_event", DESCENDING)])
    collection.create_index([("last_event", DESCENDING)])
    
    logger.info("Daily stats collection initialized")

def init_collections(db: MongoDB) -> bool:
    """Initialize all collections for Distill DB"""
    try:
        # Initialize each collection
        init_cam_groups_collection(db)
        init_cameras_collection(db)
        init_face_identities_collection(db)
        init_face_events_collection(db)
        init_daily_stats_collection(db)
        
        logger.info("All collections initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing collections: {str(e)}")
        return False

def main():
    """Main initialization function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Initialize Distill DB')
    parser.add_argument('reset', action='store_true', help='Drop existing collections')
    parser.add_argument(
        '--host',
        default='127.0.0.1',
        help='MongoDB host'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=27017,
        help='MongoDB port'
    )
    parser.add_argument(
        '--database',
        default='distill_db',
        help='Database name'
    )
    parser.add_argument(
        '--username',
        default='',
        help='MongoDB username'
    )
    parser.add_argument(
        '--password',
        default='',
        help='MongoDB password'
    )
    
    args = parser.parse_args()
    
    try:

        # Initialize MongoDB connection
        config = MongoConfig(
            host=args.host,
            port=args.port,
            database=args.database,
            username=args.username,
            password=args.password
        )
        db = MongoDB(config)

        print(args.reset)
        if args.reset:
            logger.warning(f"Dropping database: {args.database}")
            db.client.drop_database(args.database)
            logger.info("Database dropped successfully")
        
        # Initialize collections
        success = init_collections(db)
        
        if success:
            logger.info("Database initialization completed successfully")
            return 0
        else:
            logger.error("Database initialization failed")
            return 1
            
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1
    finally:
        db.close()

if __name__ == "__main__":
    """
    Usage:
    1. Normal initialization:
       python -m src.db.mongo.distill_db_init
    
    2. Reset and reinitialize:
       python -m src.db.mongo.distill_db_init --reset
    
    3. Custom configuration:
       python -m src.db.mongo.distill_db_init \
           --host localhost \
           --port 27017 \
           --database distill_db \
           --username myuser \
           --password mypass
    """
    exit(main())