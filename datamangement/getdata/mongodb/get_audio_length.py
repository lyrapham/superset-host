import json
import os

# Get audio length from Time Range
def get_audio_length(file_path):
    try:
        print("\n Getting Audio Length by ID....")
        # Load JSON file
        with open(file_path, "r", encoding="utf-8") as file:
            json_data = json.load(file)

        results = []

        for record in json_data:
            if "_id" in record and "annotation" in record and "data" in record["annotation"]:
                last_time_range = None

                # Divide last time_rage for 60 seconds
                for item in record["annotation"]["data"]:
                    if "time_range" in item:
                        last_time_range = item["time_range"]

                if last_time_range:
                    last_value = last_time_range[-1] 
                    audio_length = last_value / 60  

                    results.append({
                        "_id": record["_id"],
                        #"time_range": last_time_range, 
                        "audio_length": audio_length
                    })
        print(" Audio Length by ID has been saved to data/mongodb/bi_requirements/audio_length.json")

    except FileNotFoundError:
        print(f" Error: The file '{file_path}' was not found.")
    except json.JSONDecodeError:
        print(f" Error: Failed to parse JSON. Please check the format of '{file_path}'.")
    except Exception as e:
        print(f" Unexpected error: {e}")