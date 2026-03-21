import argparse
import configparser
import threading
import logging
import os
import csv
import base64
import html
import sys
from typing import Optional, Any
from simple_salesforce import Salesforce
from rich.console import Console


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from reporting import reporter

csv_writer_lock = threading.Lock()
console: Console = Console()

def setup_logging() -> None:
    """
    Configures logging to both console and a deploy.log file.
    """
    log_file = 'deploy.log'
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    # File handler
    fh = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Stream handler
    sh = logging.StreamHandler(sys.__stdout__)
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

def upload_files_from_csv(
    sf: Salesforce,
    args: argparse.Namespace,
    folder_output_directory: str,
    csv_filename: str = 'files.csv'
) -> None:
    object_name = args.sourceobject
    csv_path = os.path.join(folder_output_directory, csv_filename)
    processed = 0

    if not os.path.isfile(csv_path):
        logging.error(f"CSV file not found: {csv_path}")
        reporter.log(object_name, "File not found for upload and linking")
        return

    with open(csv_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for index, row in enumerate(reader, start=1):
            logging.info(f"=== Processing Row\n: {row}")
            try:
                title = row['Title'].strip()
                file_path = row['PathOnClient'].strip()
                linked_entity_id = row['FirstPublicationId'].strip()
                ContentVersionOldId = row['ContentVersionOldId'].strip()
                FirstPublicationId = row['FirstPublicationId'].strip()
                linked_entity_type = row.get('LinkedEntityType', '').strip()
                case_record_type_name = row.get('CaseRecordTypeName', '').strip()
                case_contact_id = row.get('CaseContactId', '').strip()
                is_snote = file_path.lower().endswith('.snote')
                owner_id = row.get('OwnerId', '').strip()
                logging.info(f"=== Processing File #{index} for {object_name} ===")
                logging.info(f"Title: {title}")
                logging.info(f"Path: {file_path}")
                logging.info(f"Linked Record ID: {linked_entity_id if linked_entity_id else 'None'}")
                logging.info(f"Is SNote: {'Yes' if is_snote else 'No'}")

                if not os.path.isfile(file_path):
                    logging.error(f"File not found: {file_path}")
                    reporter.log(object_name, "File not found for upload and linking")
                    continue

                try:
                    with open(file_path, 'rb') as f:
                        file_bytes = f.read()
                except Exception as e:
                    logging.error(f"Failed to read file: {file_path} | Exception: {e}")
                    reporter.log(object_name, "File can't be read")
                    continue

                if is_snote:
                    try:
                        html_text = file_bytes.decode('utf-8')
                        #escaped_html = html.escape(html_text)
                        escaped_html = html_text
                        encoded_file = base64.b64encode(escaped_html.encode('utf-8')).decode('utf-8')
                        logging.info(f"Escaped SNote and base64-encoded (length: {len(encoded_file)} chars)")
                    except UnicodeDecodeError as e:
                        logging.warning(f"Could not decode .snote file as UTF-8: {file_path} | Exception: {e}")
                        reporter.log(object_name, "Unable to decode snote file")
                        continue
                else:
                    encoded_file = base64.b64encode(file_bytes).decode('utf-8')
                    logging.info(f"File base64-encoded (length: {len(encoded_file)} chars)")

                #content_title = f"SNOTE - {title}" if is_snote else title
                content_title = title
                filename = os.path.basename(file_path)

                logging.info("Uploading to Salesforce ContentVersion...")

                try:
                    # Check if the file already exists
                    query = f"SELECT Id FROM ContentVersion WHERE SI_Old_Id__c = '{ContentVersionOldId}'"
                    existing = sf.query(query)

                    if existing['totalSize'] > 0:
                        logging.warning(f"Skipping upload (already exists): SI_Old_Id__c = {ContentVersionOldId}")
                        reporter.log(object_name, "File already exists")
                    else:
                        target_owner_id = None
                        target_owner_name = ''
                        logging.info(f"Going to check for owner id {owner_id} in target org")
                        if owner_id:
                            # Query for User with AboutMe = owner_id
                            logging.info(f"Will search for target owner id for sourceid {owner_id}")
                            user_result = sf.query(f"SELECT Id, Name FROM User WHERE AboutMe = '{owner_id}' LIMIT 1")
                            if user_result['totalSize'] > 0:
                                target_owner_id = user_result['records'][0]['Id']
                                target_owner_name = user_result['records'][0]['Name']
                                logging.info(f"Target owner id and name: {target_owner_id} {target_owner_name}")
                            else:
                                logging.warning(f"No User found in target org with AboutMe = '{owner_id}'")
                        content_version_payload = {
                            'Title': content_title,
                            'PathOnClient': filename,
                            'VersionData': encoded_file,
                            'SI_Old_Id__c': ContentVersionOldId
                        }
                        if target_owner_id:
                            content_version_payload['OwnerId'] = target_owner_id
                            logging.info(f"Will set ownerid of file = {ContentVersionOldId} to {target_owner_id}")
                        else:
                            logging.info(f"Did not find target_owner_id for = {ContentVersionOldId} ")
                        content_version = sf.ContentVersion.create(content_version_payload)
                        content_version_id = content_version.get('id')
                        logging.info(f"Uploaded file: {filename} → ContentVersionId: {content_version_id}")

                        if linked_entity_id:
                            try:
                                logging.info("Linking ContentDocument to record...")
                                query = f"SELECT ContentDocumentId FROM ContentVersion WHERE Id = '{content_version_id}'"
                                cd_id = sf.query(query)['records'][0]['ContentDocumentId']

                                # -- CUSTOM LOGIC FOR CASE > STUDENT RECORD --
                                logging.info("Checking for Case special case...")
                                if (
                                    linked_entity_type == "Case"
                                    and case_record_type_name == "Student Record"
                                    and case_contact_id
                                ):
                                    logging.info(f"Found case with Student Record type {case_contact_id}")
                                    # 1. Find Person Account
                                    acct_query = (
                                        f"SELECT Id FROM Account WHERE SI_Old_Id__c = '{case_contact_id}' LIMIT 1"
                                    )
                                    acct_result = sf.query(acct_query)
                                    if acct_result['totalSize'] > 0:
                                        person_account_id = acct_result['records'][0]['Id']
                                        # Link to Person Account
                                        sf.ContentDocumentLink.create({
                                            'ContentDocumentId': cd_id,
                                            'LinkedEntityId': person_account_id,
                                            'ShareType': 'V',
                                            'Visibility': 'AllUsers'
                                        })
                                        logging.info(
                                            f"Linked file to Person Account {person_account_id} (from Case/Student Record)")
                                        reporter.log(object_name, "Linked to Person Account")
                                    else:
                                        logging.error(
                                            f"Could not find Person Account for Case ContactId={case_contact_id} ")
                                        reporter.log(object_name, "Could not find Person Account for CaseContactId")
                                else:
                                    # Default: link to new record by FirstPublicationId
                                    query = (
                                        f"SELECT Id FROM {args.targetobject} WHERE SI_Old_Id__c = '{FirstPublicationId}' LIMIT 1"
                                    )
                                    result = sf.query(query)
                                    if result['totalSize'] > 0:
                                        newrecord_id = result['records'][0]['Id']
                                        logging.info(f"Found New Record Id: {newrecord_id}")

                                        sf.ContentDocumentLink.create({
                                            'ContentDocumentId': cd_id,
                                            'LinkedEntityId': newrecord_id,
                                            'ShareType': 'V',
                                            'Visibility': 'AllUsers'
                                        })
                                        logging.info(f"Linked to record: {linked_entity_id}")
                                    else:
                                        logging.error(
                                            f"No matching New Record found for {args.sourceobject} with Id: {FirstPublicationId}."
                                        )
                                        reporter.log(object_name, "No matching record for source object")
                            except Exception as e:
                                logging.warning(f"Failed to link file to record: {e}", exc_info=True)
                                reporter.log(object_name, "Unknown exception linking record")

                        logging.info("Done with this file.")
                    processed += 1

                except Exception as e:
                    logging.error(f"Failed to upload file to Salesforce: {e}", exc_info=True)
                    reporter.log(object_name, "Unknown exception uploading file")
                    continue

            except Exception as outer_ex:
                import traceback
                tb = traceback.format_exc()
                logging.error(f"Unhandled exception in row #{index} ({file_path}): {outer_ex}\n{tb}")
                reporter.log(object_name, f"Unhandled exception in upload_files_from_csv: {outer_ex}")
                continue

def main() -> None:
    """
    Main function to parse arguments, set up config and logging, connect to Salesforce,
    and initiate the upload process.
    """
    setup_logging()
    parser = argparse.ArgumentParser(description='Deploy ContentVersion (Files) to Salesforce Target Org')
    parser.add_argument('-so', '--sourceobject', metavar='sourceobject', required=False, default='', help='Source Object')
    parser.add_argument('-to', '--targetobject', metavar='targetobject', required=False, default='', help='Target Object')
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read('config.ini')

    username = config['salesforce']['target_username']
    password = config['salesforce']['target_password']
    token = config['salesforce']['target_security_token']

    is_sandbox = config['salesforce']['target_connect_to_sandbox']
    domain = 'test' if is_sandbox == 'True' else 'login'

    custom_domain = config['salesforce']['target_domain']
    if custom_domain:
        domain = f'{custom_domain}.my'

    batch_size = int(config['salesforce']['batch_size'])
    output_directory = config['salesforce']['output_dir']
    folder_output_directory = os.path.join(output_directory, args.sourceobject)

    if not os.path.isdir(folder_output_directory):
        os.makedirs(folder_output_directory)
        logging.info(f"Created folder: {folder_output_directory}")

    logging.info('Deploying ContentVersion (Files) to Salesforce')
    logging.info(f'Username: {username}')
    logging.info(f'Signing in at: https://{domain}.salesforce.com')

    try:
        sf = Salesforce(username=username, password=password, security_token=token, domain=domain)
        logging.info(f"Connected successfully to {sf.sf_instance}")
    except Exception as ex:
        logging.error(f"Failed to connect to Salesforce: {ex}", exc_info=True)
        print(f"[ERROR] Failed to connect to Salesforce: {ex}")
        return

    upload_files_from_csv(sf, args, folder_output_directory)

if __name__ == '__main__':
    main()
