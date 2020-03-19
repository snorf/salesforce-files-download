import concurrent.futures
from simple_salesforce import Salesforce
import requests
import os.path
import csv
import logging


def split_into_batches(l, n):
    full_list = list(l)
    for i in range(0, len(full_list), n):
        yield full_list[i:i + n]


def create_filename(title, file_extension, content_document_id, output_directory):
    # Create filename
    bad_chars = [';', ':', '!', "*", '/', '\\']
    clean_title = filter(lambda i: i not in bad_chars, title)
    clean_title = ''.join(list(clean_title))
    filename = "{0}{1} {2}.{3}".format(output_directory, content_document_id, clean_title, file_extension)
    return filename


def get_content_document_ids(sf, output_directory, query):
    # Locate/Create output directory
    if not os.path.isdir(output_directory):
        os.mkdir(output_directory)

    results_path = output_directory + 'files.csv'
    content_document_ids = set()
    content_documents = sf.query_all(query)

    # Save results file with file mapping and return ids
    with open(results_path, 'w', encoding='UTF-8', newline='') as results_csv:
        filewriter = csv.writer(results_csv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(['LinkedEntityId', 'ContentDocumentId', 'Filepath', 'PathOnClient', 'Title'])

        for content_document in content_documents["records"]:
            content_document_ids.add(content_document["ContentDocumentId"])
            filename = create_filename(content_document["ContentDocument"]["Title"],
                                       content_document["ContentDocument"]["FileExtension"],
                                       content_document["ContentDocumentId"],
                                       output_directory)

            filewriter.writerow(
                [content_document["LinkedEntityId"], content_document["ContentDocumentId"], filename, filename,
                 content_document["ContentDocument"]["Title"]])

    return content_document_ids


def download_file(args):
    record, output_directory, sf = args
    filename = create_filename(record["Title"], record["FileExtension"], record["ContentDocumentId"], output_directory)
    url = "https://%s%s" % (sf.sf_instance, record["VersionData"])

    logging.debug("Downloading from " + url)
    response = requests.get(url, headers={"Authorization": "OAuth " + sf.session_id,
                                          "Content-Type": "application/octet-stream"})

    if response.ok:
        # Save File
        with open(filename, "wb") as output_file:
            output_file.write(response.content)
        return "Saved file to %s" % filename
    else:
        return "Couldn't download %s" % url


def fetch_files(sf, query_string, output_directory, valid_content_document_ids=None, batch_size=100):
    # Divide the full list of files into batches of 100 ids
    batches = list(split_into_batches(valid_content_document_ids, batch_size))

    i = 0
    for batch in batches:

        i = i + 1
        logging.info("Processing batch {0}/{1}".format(i, len(batches)))
        batch_query = query_string + ' AND ContentDocumentId in (' + ",".join("'" + item + "'" for item in batch) + ')'
        query_response = sf.query(batch_query)
        records_to_process = len(query_response["records"])
        logging.debug("Content Version Query found {0} results".format(records_to_process))

        while query_response:
            extracted = 0
            with concurrent.futures.ProcessPoolExecutor() as executor:
                args = ((record, output_directory, sf) for record in query_response["records"])
                for result in executor.map(download_file, args):
                    logging.debug(result)
            break

        logging.debug('All files in batch {0} downloaded'.format(i))
    logging.debug('All batches complete')


def main():
    import argparse
    import configparser

    parser = argparse.ArgumentParser(description='Export ContentVersion (Files) from Salesforce')
    parser.add_argument('-q', '--query', metavar='query', required=True,
                        help='SOQL to limit the valid ContentDocumentIds. Must return the Id of related/parent objects.')
    args = parser.parse_args()

    # Get settings from config file
    config = configparser.ConfigParser()
    config.read('download.ini')

    username = config['salesforce']['username']
    password = config['salesforce']['password']
    token = config['salesforce']['security_token']
    batch_size = int(config['salesforce']['batch_size'])
    is_sandbox = config['salesforce']['connect_to_sandbox']
    loglevel = logging.getLevelName(config['salesforce']['loglevel'])
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=loglevel)

    content_document_query = 'SELECT ContentDocumentId, LinkedEntityId, ContentDocument.Title, ContentDocument.FileExtension FROM ContentDocumentLink WHERE LinkedEntityId in ({0})'.format(
        args.query)
    output = config['salesforce']['output_dir']
    query = "SELECT ContentDocumentId, Title, VersionData, CreatedDate, FileExtension FROM ContentVersion WHERE IsLatest = True AND FileExtension != 'snote'";

    domain = None
    if is_sandbox == 'True':
        domain = 'test';

    # Output
    logging.info('Export ContentVersion (Files) from Salesforce')
    logging.info('Username: ' + username)
    logging.info('Output directory: ' + output)

    # Connect
    sf = Salesforce(username=username, password=password, security_token=token, domain=domain)
    logging.debug("Connected successfully to {0}".format(sf.sf_instance))

    # Get Content Document Ids
    logging.debug("Querying to get Content Document Ids...")
    valid_content_document_ids = None
    if content_document_query:
        valid_content_document_ids = get_content_document_ids(sf=sf, output_directory=output,
                                                              query=content_document_query)
    logging.info("Found {0} total files".format(len(valid_content_document_ids)))

    # Begin Downloads
    fetch_files(sf=sf, query_string=query, valid_content_document_ids=valid_content_document_ids,
                output_directory=output, batch_size=batch_size)


if __name__ == "__main__":
    main()
