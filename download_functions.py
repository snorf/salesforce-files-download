import concurrent.futures
import hashlib
import os
import csv
import logging
import threading
import argparse
import configparser
import time
from typing import List, Dict, Any, Tuple, Generator, Optional

try:
    from simple_salesforce import Salesforce
    import requests
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, SpinnerColumn, TextColumn
except ImportError as e:
    print("Required package missing: {}".format(e))
    exit(1)

from filename_utils import create_filename, sanitize_filename

LOG_FILE = 'download_functions.log'
logging.basicConfig(
    filename=LOG_FILE,
    filemode='a',
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

csv_writer_lock = threading.Lock()
filename_lock = threading.Lock()
used_filenames: Dict[str, int] = {}


def reserve_unique_filename(filename: str) -> str:
    """
    Returns a unique filename by appending _1, _2, etc. if the base name is already taken.
    Thread-safe — tracks all filenames within a run.
    """
    with filename_lock:
        if filename not in used_filenames:
            used_filenames[filename] = 0
            return filename
        used_filenames[filename] += 1
        count = used_filenames[filename]
        base, ext = os.path.splitext(filename)
        unique = f"{base}_{count}{ext}"
        # Ensure the suffixed name is also unique
        while unique in used_filenames:
            count += 1
            used_filenames[filename] = count
            unique = f"{base}_{count}{ext}"
        used_filenames[unique] = 0
        return unique


def split_into_batches(items: List[Any], batch_size: int) -> Generator[List[Any], None, None]:
    full_list = list(items)
    for i in range(0, len(full_list), batch_size):
        yield full_list[i:i + batch_size]




def preflight_checks(config: configparser.ConfigParser, folder_output_directory: str) -> None:
    errors = []
    if not os.path.isdir(folder_output_directory):
        try:
            os.makedirs(folder_output_directory)
            logger.info(f"Created output directory: {folder_output_directory}")
        except Exception as ex:
            errors.append(f"Could not create output directory {folder_output_directory}: {ex}")
    if errors:
        for e in errors:
            logger.error(e)
            print(f"[ERROR] {e}")
        exit(1)


def fetch_case_fields(sf, case_id: str) -> Tuple[str, str]:
    """
    Returns (RecordType.Name, ContactId) for a given Case Id.
    If not a Case or not found, returns ("", "").
    """
    try:
        result = sf.query(
            f"SELECT RecordType.Name, ContactId FROM Case WHERE Id = '{case_id}'"
        )
        if result['records']:
            rec = result['records'][0]
            rec_type = rec.get('RecordType', {}).get('Name', '')
            contact_id = rec.get('ContactId', '')
            return rec_type, contact_id
    except Exception as e:
        logger.warning(f"Failed to fetch Case fields for Id {case_id}: {e}")
    return "", ""


def download_file(args: Tuple) -> str:
    (
        record, folder_output_directory, sf, results_path,
        content_document_links, content_document_id_name, filename_pattern
    ) = args

    content_version_old_id = record.get("Id", "UNKNOWN")
    content_document_id = record.get("ContentDocumentId", "UNKNOWN")
    title = record.get("Title", "UNKNOWN")
    file_extension = record.get("FileExtension", "")
    owner_id = record.get("OwnerId", "")
    version_number = record.get("VersionNumber", "")

    try:
        # Find link info
        content_document_link = next(
            (cdl for cdl in content_document_links if cdl.get(content_document_id_name) == content_document_id),
            {}
        )
        linked_entity_id = content_document_link.get("LinkedEntityId", content_document_link.get("Id", ""))
        linked_entity_name = content_document_link.get("LinkedEntity", {}).get("Name", content_document_link.get("Title", ""))

        # Guess the type by Id prefix (Case: '500')
        linked_entity_type = ""
        case_record_type_name = ""
        case_contact_id = ""
        if linked_entity_id and str(linked_entity_id).startswith('500'):
            linked_entity_type = "Case"
            case_record_type_name, case_contact_id = fetch_case_fields(sf, linked_entity_id)
        elif linked_entity_id:
            linked_entity_type = ""  # Not used for non-Cases

        filename = create_filename(
            output_directory=folder_output_directory,
            content_document_id=content_document_id,
            title=title,
            file_extension=file_extension,
            linked_entity_name=linked_entity_name,
            version_number=version_number,
            filename_pattern=filename_pattern
        )
        filename = reserve_unique_filename(filename)

        # Skip if file exists and checksum matches (no point re-downloading identical files)
        skipped = False
        if os.path.exists(filename):
            sf_checksum = record.get("Checksum", "")
            if sf_checksum:
                with open(filename, "rb") as f:
                    local_md5 = hashlib.md5(f.read()).hexdigest()
                if local_md5 == sf_checksum:
                    logging.debug(f"Skipped (checksum match): {filename}")
                    skipped = True
                else:
                    logging.info(f"Checksum mismatch for {filename}, re-downloading (local={local_md5}, sf={sf_checksum})")
            else:
                # No checksum from Salesforce, skip based on file existence
                logging.debug(f"Skipped (file exists): {filename}")
                skipped = True

        if not skipped:
            url = f"https://{getattr(sf, 'sf_instance', 'dummy.salesforce.com')}{record.get('VersionData', '')}"

            logging.debug("Downloading from " + url)
            response = requests.get(url, headers={"Authorization": "OAuth " + sf.session_id,
                                                  "Content-Type": "application/octet-stream"})
            if response.ok:
                try:
                    with open(filename, "wb") as output_file:
                        output_file.write(response.content)
                    logger.info(f"Saved file to {filename}")
                except Exception as ex:
                    logger.error(f"Error saving file {filename}: {ex}")
                    return f"Error saving file {filename}: {ex}"
            else:
                msg = f"Couldn't download {url}. Status: {response.status_code}"
                logger.error(msg)
                return msg

        # Write file entry to csv (for both downloaded and skipped files)
        with csv_writer_lock:
            try:
                with open(results_path, 'a', encoding='UTF-8', newline='') as results_csv:
                    filewriter = csv.writer(results_csv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
                    filewriter.writerow([
                        content_version_old_id, linked_entity_id, linked_entity_name, content_document_id, title,
                        filename, filename,
                        linked_entity_type,      # Case or ""
                        case_record_type_name,   # If Case
                        case_contact_id,          # If Case
                        owner_id
                    ])
            except Exception as ex:
                logger.error(f"Error writing CSV for {filename}: {ex}")
                return f"Error writing CSV for {filename}: {ex}"
        return "Skipped" if skipped else f"Saved file to {filename}"
    except Exception as ex:
        logger.error(f"Exception downloading file {content_document_id}: {ex}")
        return f"Exception: {ex}"


def fetch_files(
    sf: Any,
    content_document_links: Optional[List[Dict[str, Any]]] = None,
    folder_output_directory: Optional[str] = None,
    results_path: Optional[str] = None,
    filename_pattern: Optional[str] = None,
    content_document_id_name: str = 'ContentDocumentId',
    batch_size: int = 100,
    file_extension_filter: Optional[str] = None
) -> None:
    batches = list(split_into_batches(content_document_links or [], batch_size))
    used_filenames.clear()

    # Count total files across all batches for progress bar
    total_files = 0
    batch_records: List[List[Dict[str, Any]]] = []
    for i, batch in enumerate(batches, 1):
        logging.info("Processing batch {0}/{1}".format(i, len(batches)))

        if file_extension_filter:
            query_string = (
                "SELECT Id, ContentDocumentId, Title, VersionData, FileExtension, OwnerId, VersionNumber, Checksum "
                "FROM ContentVersion "
                f"WHERE IsLatest = True AND FileType IN ({file_extension_filter})"
            )
        else:
            query_string = (
                "SELECT Id, ContentDocumentId, Title, VersionData, FileExtension, OwnerId, VersionNumber, Checksum "
                "FROM ContentVersion "
                "WHERE IsLatest = True AND FileExtension != 'snote'"
            )

        batch_query = (
            query_string +
            ' AND ContentDocumentId in (' +
            ",".join("'" + item[content_document_id_name] + "'" for item in batch) +
            ') ORDER BY CreatedDate ASC'
        )

        query_response = sf.query(batch_query)
        records = query_response.get("records", [])
        logging.debug("Content Version Query found {0} results".format(len(records)))

        if not records:
            logging.info(f"No files found in batch {i}")
            batch_records.append([])
            continue

        batch_records.append(records)
        total_files += len(records)

    if not total_files:
        logging.info("No files to download")
        return {"total": 0, "success": 0, "failed": 0, "skipped": 0, "duration": 0.0}

    start_time = time.time()
    success_count = 0
    failed_count = 0
    skipped_count = 0

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=Console(force_terminal=True),
    )

    with progress:
        task_id = progress.add_task("[cyan]Downloading files...", total=total_files)

        for i, (batch, records) in enumerate(zip(batches, batch_records), 1):
            if not records:
                continue

            args_list = [
                (
                    record, folder_output_directory, sf, results_path, batch, content_document_id_name, filename_pattern
                ) for record in records
            ]

            with concurrent.futures.ThreadPoolExecutor() as executor:
                for result in executor.map(download_file, args_list):
                    logging.debug(result)
                    if result and result.startswith("Saved"):
                        success_count += 1
                    elif result and result.startswith("Skipped"):
                        skipped_count += 1
                    else:
                        failed_count += 1
                    progress.advance(task_id)

    duration = time.time() - start_time
    logging.info('All batches complete')
    return {"total": total_files, "success": success_count, "failed": failed_count, "skipped": skipped_count, "duration": duration}


def main():
    parser = argparse.ArgumentParser(description='Export ContentVersion (Files) from Salesforce')
    parser.add_argument('-q', '--query', metavar='query', required=True,
                        help='SOQL to limit the valid ContentDocumentIds. Must return the Ids of parent objects.')
    parser.add_argument('-o', '--object', metavar='object', required=False, default='ContentDocumentLink',
                        help="How are the ContentDocument selected, via 'ContentDocumentLink' (default) or directly from 'ContentDocument'")
    parser.add_argument('-f', '--filenamepattern', metavar='filenamepattern', required=False, default='{0}{1}-{2}.{3}',
                        help='Specify filename pattern: {0}=output_directory, {1}=content_document_id, {2}=title, {3}=file_extension, {4}=linked_entity_name, {5}=version_number')
    parser.add_argument('-so', '--sourceobject', metavar='sourceobject', required=True, default='',
                        help='Source object for downloaded object')
    parser.add_argument('-fe', '--filter_file_extension', metavar='filter_file_extension', required=False, default=None,
                        help="Filter by file type, e.g. \"'EXCEL_M', 'EXCEL'\" (Salesforce FileType values)")
    args, extra = parser.parse_known_args()

    config = configparser.ConfigParser(allow_no_value=True)
    config.read('config.ini')

    username = config['salesforce']['source_username']
    password = config['salesforce']['source_password']
    token = config['salesforce']['source_security_token']

    is_sandbox = config['salesforce']['source_connect_to_sandbox']
    if is_sandbox == 'True':
        domain = 'test'
    else:
        domain = 'login'
    domain_config = config['salesforce']['source_domain']
    if domain_config:
        domain = domain_config + '.my'

    batch_size = int(config['salesforce']['batch_size'])
    loglevel = logging.getLevelName(config['salesforce']['loglevel'])
    output_directory = config['salesforce']['output_dir']
    folder_output_directory = os.path.join(output_directory, args.sourceobject) + "/"

    logging.getLogger().setLevel(loglevel)
    logger.info('Export ContentVersion (Files) from Salesforce')
    logger.info('Username: ' + username)
    logger.info('Signing in at: https://' + domain + '.salesforce.com')

    preflight_checks(config, folder_output_directory)

    sf = Salesforce(username=username, password=password, security_token=token, domain=domain)
    logging.debug("Connected successfully to {0}".format(sf.sf_instance))

    logger.info('Output directory: ' + folder_output_directory)
    results_path = os.path.join(folder_output_directory, 'files.csv')
    csv_header = [
        'ContentVersionOldId', 'FirstPublicationId', 'FirstPublicationName', 'ContentDocumentId', 'Title',
        'VersionData', 'PathOnClient',
        'LinkedEntityType',      # Case or blank
        'CaseRecordTypeName',    # Case RecordType.Name or blank
        'CaseContactId',          # Case ContactId or blank
        'OwnerId'
    ]
    with open(results_path, 'w', encoding='UTF-8', newline='') as results_csv:
        filewriter = csv.writer(results_csv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(csv_header)

    # Use generic SOQL for all objects, so no SOQL errors for non-Case objects
    if args.object == 'ContentDocumentLink':
        content_document_query = (
            'SELECT ContentDocumentId, LinkedEntityId, LinkedEntity.Name, ContentDocument.Title, ContentDocument.FileExtension '
            'FROM ContentDocumentLink '
            f'WHERE LinkedEntityId in ({args.query}) AND ContentDocument.IsDeleted = false'
        )
        content_document_id_name = 'ContentDocumentId'
    elif args.object == 'ContentDocument':
        content_document_query = f'SELECT Id, Title, FileExtension FROM ContentDocument {args.query}'.strip()
        content_document_id_name = 'Id'
    else:
        raise ValueError(f'Invalid QueryType {args.object}')

    content_document_links = sf.query_all(content_document_query)["records"]
    logger.info("Found %s total files", len(content_document_links))
    print(f"Found {len(content_document_links)} total files")

    # Get file extension filter from args or config
    file_extension_filter = args.filter_file_extension or config['salesforce'].get('default_file_extension_filter', '')

    stats = fetch_files(
        sf=sf,
        content_document_links=content_document_links,
        folder_output_directory=folder_output_directory,
        results_path=results_path,
        batch_size=batch_size,
        content_document_id_name=content_document_id_name,
        filename_pattern=args.filenamepattern,
        file_extension_filter=file_extension_filter if file_extension_filter else None
    )

    if stats and stats["total"] > 0:
        duration = stats["duration"]
        minutes, seconds = divmod(duration, 60)
        downloaded = stats["success"] + stats["failed"]
        files_per_sec = downloaded / duration if duration > 0 and downloaded > 0 else 0
        print(f"\nDownload complete: {stats['success']} succeeded, {stats['failed']} failed, {stats['skipped']} skipped out of {stats['total']} files")
        print(f"Duration: {int(minutes)}m {seconds:.1f}s" + (f" ({files_per_sec:.1f} files/sec)" if files_per_sec > 0 else ""))
        print(f"Output: {folder_output_directory}")
    else:
        print("No files to download")
    print(f"Object Completed")


if __name__ == "__main__":
    main()
