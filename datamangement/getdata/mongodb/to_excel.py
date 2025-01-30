import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from openpyxl import load_workbook
import os
import logging

# Configure logging
logging.basicConfig(
    filename='data_sync.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Google Sheets Credentials Setup
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_FILE = "credentials.json"  # Path to your Google API credentials

# Authenticate Google Sheets API
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)

# Find the existing Google Sheets file by name
spreadsheet_name = "inuse_token"
spreadsheet = None

try:
    spreadsheet = client.open(spreadsheet_name)
    logging.info(f"Opened existing Google Sheets file: {spreadsheet_name}")
except gspread.exceptions.SpreadsheetNotFound:
    logging.error(f"Spreadsheet '{spreadsheet_name}' not found. Please check the name.")
    raise SystemExit(f"Spreadsheet '{spreadsheet_name}' not found.")

def json_to_google_sheets(audio_json_path, chat_json_path, referral_json_path, speaker_json_path, conversation_json_path):
    """
    Converts multiple JSON files to DataFrames and uploads them to a Google Sheets file.
    """
    # Load JSON data into Pandas DataFrames
    def load_json(json_path):
        with open(json_path, "r") as file:
            return json.load(file)

    audio_data = load_json(audio_json_path)
    chat_data = load_json(chat_json_path)
    referral_data = load_json(referral_json_path)
    speaker_data = load_json(speaker_json_path)
    conversation_data = load_json(conversation_json_path)

    # Convert to Pandas DataFrames
    df_audio = pd.DataFrame(audio_data)
    
    extracted_chat_data = [
        {
            "_id": entry.get("_id", ""),
            "Completion Tokens": entry.get("chat_history_tokens", {}).get("completion_tokens", 0),
            "Prompt Tokens": entry.get("chat_history_tokens", {}).get("prompt_tokens", 0),
            "Total Tokens": entry.get("chat_history_tokens", {}).get("total_tokens", 0),
        }
        for entry in chat_data
    ]
    df_chat = pd.DataFrame(extracted_chat_data)

    extracted_referral_data = [
        {
            "_id": entry.get("_id", ""),
            "Completion Tokens": entry.get("referral_letter", {}).get("completion_tokens", 0),
            "Prompt Tokens": entry.get("referral_letter", {}).get("prompt_tokens", 0),
            "Total Tokens": entry.get("referral_letter", {}).get("total_tokens", 0),
        }
        for entry in referral_data
    ]
    df_referral = pd.DataFrame(extracted_referral_data)

    extracted_speaker_data = [
        {
            "_id": entry.get("_id", ""),
            "Completion Tokens": entry.get("speaker_annotation", {}).get("completion_tokens", 0),
            "Prompt Tokens": entry.get("speaker_annotation", {}).get("prompt_tokens", 0),
            "Total Tokens": entry.get("speaker_annotation", {}).get("total_tokens", 0),
        }
        for entry in speaker_data
    ]
    df_speaker = pd.DataFrame(extracted_speaker_data)

    extracted_conversation_data = [
        {
            "_id": entry.get("_id", ""),
            "Short SOAP Tokens": entry.get("conversation_summary", {}).get("short", {}).get("soap", {}).get("total_tokens", 0),
            "Short Standard Tokens": entry.get("conversation_summary", {}).get("short", {}).get("standard", {}).get("total_tokens", 0),
            "Medium SOAP Tokens": entry.get("conversation_summary", {}).get("medium", {}).get("soap", {}).get("total_tokens", 0),
            "Medium Standard Tokens": entry.get("conversation_summary", {}).get("medium", {}).get("standard", {}).get("total_tokens", 0),
            "Long SOAP Tokens": entry.get("conversation_summary", {}).get("long", {}).get("soap", {}).get("total_tokens", 0),
            "Long Standard Tokens": entry.get("conversation_summary", {}).get("long", {}).get("standard", {}).get("total_tokens", 0),
        }
        for entry in conversation_data
    ]
    df_conversation = pd.DataFrame(extracted_conversation_data)

    # Upload to Google Sheets
    def update_sheet(sheet_name, dataframe):
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            worksheet.clear()  # Clear old data
            worksheet.update([dataframe.columns.tolist()] + dataframe.values.tolist())  # Upload new data
            logging.info(f"Updated {sheet_name} in Google Sheets.")
        except gspread.exceptions.WorksheetNotFound:
            # If the sheet does not exist, create a new one
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
            worksheet.update([dataframe.columns.tolist()] + dataframe.values.tolist())
            logging.info(f"Created and updated new sheet {sheet_name} in Google Sheets.")

    # Push DataFrames to Google Sheets
    update_sheet("Audio Length", df_audio)
    update_sheet("Chat History", df_chat)
    update_sheet("Referral Letter", df_referral)
    update_sheet("Speaker Annotation", df_speaker)
    update_sheet("Conversation Summary", df_conversation)

    logging.info("All sheets updated successfully!")

if __name__ == "__main__":
    audio_json_path = "data/mongodb/bi_requirements/audio_length.json"
    chat_json_path = "data/mongodb/bi_requirements/chat_history_tokens_summary.json"
    referral_json_path = "data/mongodb/bi_requirements/referral_letter_tokens_summary.json"
    speaker_json_path = "data/mongodb/bi_requirements/speaker_annotation_tokens_summary.json"
    conversation_json_path = "data/mongodb/bi_requirements/conversation_summary_tokens_summary.json"

    json_to_google_sheets(audio_json_path, chat_json_path, referral_json_path, speaker_json_path, conversation_json_path)
