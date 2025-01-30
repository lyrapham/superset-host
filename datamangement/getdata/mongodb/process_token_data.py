import logging
import os
from exportdata import export_mongodb_collections
from get_audio_length import get_audio_length
from get_tokens_summary import (
    extract_speaker_annotation_token,
    extract_conversation_summary_token,
    extract_chat_history_token,
    extract_referral_letter_token
)

# Configure logging
logging.basicConfig(
    filename='main_runner.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# MongoDB Connection URI
uri = "mongodb+srv://read_only:IsMNUrJRkaDRcs3W@cluster0.stzrc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

def process_token_data():
    try:
        logging.info("Starting token data processing")
        
        # Export collections
        export_mongodb_collections(uri, db_name="gpt")
        
        # Input files
        data_folder = "data/mongodb/source_data"
        files = {
            "session_history": os.path.join(data_folder, "session_history.json"),
            "llm_call_history": os.path.join(data_folder, "llm_call_history.json"),
            "chat_history": os.path.join(data_folder, "chat_history.json"),
        }

        chat_history = files["chat_history"]
        audio_length = "data/mongodb/bi_requirements/audio_length.json"

        # Get audio length
        audio_length_file = get_audio_length(chat_history)
        logging.info(f"Audio length file processed: {audio_length_file}")

        # Run the token extraction functions
        extract_speaker_annotation_token(files['session_history'], 'data/mongodb/bi_requirements/speaker_annotation_tokens_summary.json')
        extract_conversation_summary_token(files['session_history'], 'data/mongodb/bi_requirements/conversation_summary_tokens_summary.json')
        extract_chat_history_token(files["chat_history"], 'data/mongodb/bi_requirements/chat_history_tokens_summary.json')
        extract_referral_letter_token(files['session_history'], 'data/mongodb/bi_requirements/referral_letter_tokens_summary.json')
        
        logging.info("Token data processing completed successfully")
    except Exception as e:
        logging.error(f"Error during token data processing: {e}")

if __name__ == "__main__":
    logging.info("Starting main script execution")
    
    # Run process_token_data
    process_token_data()
    
    logging.info("Finished executing all scripts")
