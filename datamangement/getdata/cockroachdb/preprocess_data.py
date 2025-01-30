import psycopg2
import pandas as pd
from datetime import datetime
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from config import db_params  # Import the db_params from config.py
import os
import logging

# Configure logging
logging.basicConfig(
    filename='data_sync.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

folder_id = "15gmgsovtXeQkWwMkAi5HU6jdwFvnzBGQ"
inuse_name = "inuse"

def preprocess_data():
    try:
        # Step 1: Connect to the database
        conn = psycopg2.connect(**db_params)
        print(conn)

        # Step 2: Query to fetch data from the user_feedback table
        query = 'SELECT * FROM public."user_feedback"'

        # Step 3: Fetch data into a pandas DataFrame
        df = pd.read_sql_query(query, conn)
        print(df)

        # Step 4: Close the database connection
        #conn.close()

        # Step 5: Convert 'created_at' and 'updated_at' columns to datetime and split into date and time
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['updated_at'] = pd.to_datetime(df['updated_at'])

        df['created_at_date'] = df['created_at'].dt.strftime('%Y-%m-%d')
        df['created_at_time'] = df['created_at'].dt.strftime('%H:%M:%S')

        df['updated_at_date'] = df['updated_at'].dt.strftime('%Y-%m-%d')
        df['updated_at_time'] = df['updated_at'].dt.strftime('%H:%M:%S')

        # Step 5.1: Update 'q4_answer' values based on the specified conditions
        def update_q4_answer(answer):
            if answer == "1" or answer == "7":
                return "1-5"
            elif answer == "2":
                return "5-10"
            elif answer == "3":
                return "10-20"
            elif answer == "4":
                return "over-20"
            elif answer == "5":
                return "0"
            else:
                return answer

        df['q4_answer'] = df['q4_answer'].apply(update_q4_answer)

        df = df.drop(columns=['created_at', 'updated_at'], errors='ignore')  # Safely drop columns if they exist

        # Step 6: Create 'timesave_min' based on 'q4_answer'
        def calculate_timesave(answer):
            if answer == "1 - 5 minutes":
                return 1
            elif answer == "5 - 10 minutes":
                return 5
            elif answer == "10 - 20 minutes":
                return 10
            elif answer == "> 20 minutes":
                return 20
            elif answer == "Not at all":
                return 0
            else:
                return None

        df['q4_answer'] = df['q4_answer'].astype('string')
        df['timesave_min'] = df['q4_answer'].apply(calculate_timesave)

        # Generate filename with current date and time
        filename = datetime.now().strftime(r"./data/survey_feedback%Y%m%d_%H%M%S.xlsx")

        # Use openpyxl to save the Excel file
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)  # Save without row indices

        logging.info(f"Data saved to {filename}")

        # Authenticate and create the PyDrive client
        client_secrets_path = os.path.join("./getdata/client_secrets.json")
        creds_path = os.path.join("./getdata/mycreds.txt")

        gauth = GoogleAuth()

        gauth.LoadCredentialsFile(creds_path)

        if not gauth.credentials or gauth.access_token_expired:
            gauth.LoadClientConfigFile(client_secrets_path)
            gauth.LocalWebserverAuth()
            gauth.SaveCredentialsFile(creds_path)
        else:
            gauth.Authorize()

        drive = GoogleDrive(gauth)

        logging.info("Authentication successful!")

        # Create a Google Drive file instance and set its content from the CSV file
        gfile = drive.CreateFile({
            'title': os.path.basename(filename),  # File name
            'parents': [{'id': folder_id}]        # Specify the folder ID
        })
        gfile.SetContentFile(filename)  # Set the file content
        gfile.Upload()  # Upload the file

        logging.info(f"Data uploaded to Google Drive as {gfile['title']}")

        # Step 1: Search for the 'inuse.xlsx' file in the specified folder
        file_list = drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()
        inuse_file = None
        for file in file_list:
            if file['title'] == inuse_name:
                inuse_file = file
                break

        # Step 2: Update or Upload the file
        if inuse_file:
            logging.info(f"Found existing file: {inuse_file['title']} (ID: {inuse_file['id']}). Updating...")
            inuse_file.SetContentFile(filename)  # Replace content with the local file
            inuse_file.Upload()  # Overwrite the file
            logging.info(f"File {inuse_file['title']} updated successfully.")
        else:
            logging.info(f"File '{inuse_name}' not found in the folder. Creating a new file...")
            new_file = drive.CreateFile({
                'title': inuse_name,
                'parents': [{'id': folder_id}]
            })
            new_file.SetContentFile(filename)
            new_file.Upload()
            logging.info(f"File {new_file['title']} uploaded successfully.")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    preprocess_data()