# Collection of session_history, chat_history, llm_call_history, request_count
# For BI purpose: Audio length data, OpenAI token usage, Number of calls 

import os
import json
import pytz
from pymongo import MongoClient
from datetime import datetime
from datetime import datetime

timezone = "America/Toronto"

def get_timezone(timezone):
    # Get the Toronto timezone
    toronto_timezone = pytz.timezone("America/Toronto")

    # Get the current time in Toronto timezone
    toronto_time = datetime.now(toronto_timezone)
    return toronto_time.strftime("%Y-%m-%d %H:%M:%S")


def export_mongodb_collections(uri, db_name, output_dir="data/mongodb/source_data"):

    # Connect to MongoDB
    client = MongoClient(uri)
    db = client[db_name]  
    print(f"\n Updating data from MongoDB at {get_timezone(timezone)}")
    print(f" Database: {db_name}")
    # Ensure the directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Get all collection names in the database
    collection_names = db.list_collection_names()

    # Function to handle datetime conversion
    def convert_datetime(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()  # Convert datetime to ISO string
        raise TypeError(f"Type {type(obj)} not serializable")

    # Loop through each collection
    for collection_name in collection_names:
        collection = db[collection_name]
        print(f"  📂 Exporting Collection: {collection_name}")

        # Fetch all documents
        documents = list(collection.find())

        # Convert ObjectId and datetime to strings (for JSON serialization)
        for doc in documents:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
            # Convert datetime fields (if any) to ISO string
            for key, value in doc.items():
                if isinstance(value, datetime):
                    doc[key] = convert_datetime(value)

        # Save data to a JSON file inside output_dir
        filename = os.path.join(output_dir, f"{collection_name}.json")
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(documents, file, indent=4, ensure_ascii=False, default=convert_datetime)

        print(f"    ✅ {filename} exported successfully!")

    # Close the connection
    client.close()
