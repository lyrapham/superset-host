"""
OpenAI token usage (prompt and completion) for each step:
    a) Speaker annotation (speaker_annotation)
    b) Clinical note summarisation (conversation_summary)
    c) Clinical note format change (eg. changing to SOAP/Standard and Short/Medium/Long) - NOT DONE
    d) Letter generation (referal_letter)
    e) Chatbot interactions (chat_history)

Number of calls to each OpenAI endpoint per consultation
"""

import json

# Extracting Speaker Annotation Token -> annotation in session_history (Not done in llm_call_history)
def extract_speaker_annotation_token(input_path, output_path):
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Prepare the list to store the extracted data
    extracted_data = []

    for record in data:
        # Extract relevant fields
        _id = record.get('_id')
        speaker_annotation = record.get('annotation')
        speaker_annotation_usage = speaker_annotation.get('usage', {})

        # Append to the result list
        extracted_data.append({
            "_id": _id,
            "speaker_annotation": speaker_annotation_usage
        })

    # Write the extracted data to a new JSON file
    with open(output_path, 'w', encoding='utf-8') as out_f:
        json.dump(extracted_data, out_f, ensure_ascii=False, indent=4)

    print(f"Extracted data has been saved to: {output_path}")

# Extracting Conversation Summary Token -> conversation_summary in session_history
def extract_conversation_summary_token(input_path, output_path):
    def extract_usage(conversation_summary, length):
        length_summary = conversation_summary.get(length, {})
        return {
            "soap": length_summary.get("soap", {}).get("usage", {}),
            "standard": length_summary.get("standard", {}).get("usage", {}),
        }

    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    extracted_data = []

    for record in data:
        _id = record.get('_id')
        conversation_summary = record.get('conversation_summary', {})
        
        # Extract data for short, medium, and long summaries
        speaker_annotation = {
            length: extract_usage(conversation_summary, length)
            for length in ["short", "medium", "long"]
        }
        
        extracted_data.append({
            "_id": _id,
            "conversation_summary": speaker_annotation
        })

    with open(output_path, 'w', encoding='utf-8') as out_f:
        json.dump(extracted_data, out_f, ensure_ascii=False, indent=4)

    print(f"Extracted data has been saved to: {output_path}")

# Extract Chat History Token --> chat_history in session_history
def extract_chat_history_token(input_path, output_path):
    import json

    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Prepare the list to store the extracted data
    extracted_data = []

    for record in data:
        # Extract relevant fields
        _id = record.get('_id', "unknown_id")
        chat_history = record.get('chat_history', {})

        # Safely handle different cases for chat_history
        if isinstance(chat_history, list) and len(chat_history) > 0:
            # If chat_history is a non-empty list, access the first element
            chat_history_usage = chat_history[0].get('token_usage', {})
        elif isinstance(chat_history, dict):
            # If chat_history is a dictionary, directly access 'token_usage'
            chat_history_usage = chat_history.get('token_usage', {})
        else:
            # If chat_history is None, empty, or unexpected, use an empty dictionary
            chat_history_usage = {}

        # Append to the result list
        extracted_data.append({
            "_id": _id,
            "chat_history_tokens": chat_history_usage
        })

    # Write the extracted data to a new JSON file
    with open(output_path, 'w', encoding='utf-8') as out_f:
        json.dump(extracted_data, out_f, ensure_ascii=False, indent=4)

    print(f"Extracted data has been saved to: {output_path}")

# Extract Referral Letter Token --> referral_letter in session_history
def extract_referral_letter_token(input_path, output_path):
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Prepare the list to store the extracted data
    extracted_data = []

    for record in data:
        # Extract relevant fields
        _id = record.get('_id')
        referral_letter = record.get('referral_letter', {})
        chat_history_usage = referral_letter.get('usage', {})
        # Append to the result list
        extracted_data.append({
            "_id": _id,
            "referral_letter": chat_history_usage
        })

    # Write the extracted data to a new JSON file
    with open(output_path, 'w', encoding='utf-8') as out_f:
        json.dump(extracted_data, out_f, ensure_ascii=False, indent=4)

    print(f"Extracted data has been saved to: {output_path}")