import sys
import os

# Ensure project root is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
print("Current sys.path:", sys.path)  # Debugging line

import logging
from mongodb.process_token_data import process_token_data
from cockroachdb.preprocess_data import preprocess_data

# Configure logging
logging.basicConfig(
    filename='main_runner.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_script(script_func, script_name):
    try:
        logging.info(f"Running {script_name}")
        script_func()
        logging.info(f"Successfully executed {script_name}")
    except Exception as e:
        logging.error(f"Error while executing {script_name}: {e}")

if __name__ == "__main__":
    logging.info("Starting main script execution")

    # Run preprocess_data
    run_script(preprocess_data, "preprocess_data")

    # Run process_token_data
    run_script(process_token_data, "process_token_data")

    logging.info("Finished executing all scripts")
