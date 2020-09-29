import concurrent.futures
from simple_salesforce import Salesforce
import requests
import os
import csv
import re
import logging
import threading

# a global lock used by the batch file downloader to write
# entries to the csv as they're downloaded 
csv_writer_lock = threading.Lock()

def split_into_batches(items, batch_size):
    full_list = list(items)
    for i in range(0, len(full_list), batch_size):
        yield full_list[i:i + batch_size]


def create_filename(title, file_extension, content_document_id, output_directory):
    # Create filename
    if ( os.name == 'nt') :
        # on windows, this is harder 
        # see https://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename

        bad_chars= re.compile(r'[^A-Za-z0-9_. ]+|^\.|\.$|^ | $|^$')
        bad_names= re.compile(r'(aux|com[1-9]|con|lpt[1-9]|prn)(\.|$)')
        clean_title = bad_chars.sub('_', title)
        if bad_names.match(clean_title) :
            clean_title = '_'+clean_title

    else :

        bad_chars = [';', ':', '!', "*", '/', '\\']
        clean_title = filter(lambda i: i not in bad_chars, title)
        clean_title = ''.join(list(clean_title))

    filename = "{0}{1} {2}.{3}".format(output_directory, content_document_id, clean_title, file_extension)
    return filename


def get_content_document_ids(sf, output_directory, query):

    results_path = output_directory + 'files.csv'
    content_document_ids = set()
    content_documents = sf.query_all(query)

    for content_document in content_documents["records"]:
        content_document_ids.add(content_document["ContentDocumentId"])
        filename = create_filename(content_document["ContentDocument"]["Title"],
                                    content_document["ContentDocument"]["FileExtension"],
                                    content_document["ContentDocumentId"],
                                    output_directory)

    return content_document_ids


def download_file(args):

    record, output_directory, sf, results_path, content_document_links = args

    content_document_id = record["ContentDocumentId"]
    title = record["Title"]
    file_extension = record["FileExtension"]

    content_document_link = next( (cdl for cdl in content_document_links if cdl["ContentDocumentId"] == content_document_id), None)
    linked_entity_id = content_document_link["LinkedEntityId"]
    linked_entity_name = content_document_link["LinkedEntity"]["Name"]

    url = "https://%s%s" % (sf.sf_instance, record["VersionData"])

    logging.debug("Downloading from " + url)
    response = requests.get(url, headers={"Authorization": "OAuth " + sf.session_id,
                                          "Content-Type": "application/octet-stream"})
    if response.ok:
        # Save File
        filename = create_filename(title, file_extension, content_document_id, output_directory)
        with open(filename, "wb") as output_file:
            output_file.write(response.content)

            # write file entry to csv
            csv_writer_lock.acquire()
            with open(results_path, 'a', encoding='UTF-8', newline='') as results_csv:
                filewriter = csv.writer(results_csv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)   
                filewriter.writerow([linked_entity_id, linked_entity_name, content_document_id , title, filename, filename])
            csv_writer_lock.release()

        return "Saved file to %s" % filename
    else:
        return "Couldn't download %s" % url


def fetch_files(sf, content_document_links=None, output_directory=None, results_path=None, batch_size=100):
    
    # Divide the full list of files into batches of 100 ids
    batches = list(split_into_batches(content_document_links, batch_size))

    i = 0
    for batch in batches:

        i = i + 1
        logging.info("Processing batch {0}/{1}".format(i, len(batches)))
        
        query_string = "SELECT ContentDocumentId, Title, VersionData, FileExtension FROM ContentVersion " \
            "WHERE IsLatest = True AND FileExtension != 'snote'"
        batch_query = query_string + ' AND ContentDocumentId in (' + ",".join("'" + item["ContentDocumentId"] + "'" for item in batch) + ')'
        query_response = sf.query(batch_query)
        records_to_process = len(query_response["records"])
        logging.debug("Content Version Query found {0} results".format(records_to_process))

        while query_response:
            with concurrent.futures.ProcessPoolExecutor() as executor:
                args = ((record, output_directory, sf, results_path, batch) for record in query_response["records"])
                
                for result in executor.map(download_file, args):
                    logging.debug(result)
            break

        logging.debug('All files in batch {0} downloaded'.format(i))
    logging.debug('All batches complete')


def main():
    import argparse
    import configparser
    import threading

    # Process command line arguments
    parser = argparse.ArgumentParser(description='Export ContentVersion (Files) from Salesforce')
    parser.add_argument('-q', '--query', metavar='query', required=True,
                        help='SOQL to limit the valid ContentDocumentIds. Must return the Ids of parent objects.')
    args = parser.parse_args()

    # Get settings from config file
    config = configparser.ConfigParser(allow_no_value=True)
    config.read('download.ini')

    username = config['salesforce']['username']
    password = config['salesforce']['password']
    token = config['salesforce']['security_token']

    is_sandbox = config['salesforce']['connect_to_sandbox']
    if is_sandbox == 'True':
        domain = 'test'    

    # custom domain overrides "test" in case of sandbox
    domain = config['salesforce']['domain']
    if domain :
        domain += '.my'
    else :
        domain = 'login'
    
    output_directory = config['salesforce']['output_dir']
    batch_size = int(config['salesforce']['batch_size'])
    loglevel = logging.getLevelName(config['salesforce']['loglevel'])  

    # Setup logging
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=loglevel)

    logging.info('Export ContentVersion (Files) from Salesforce')
    logging.info('Username: ' + username)
    logging.info('Signing in at: https://'+ domain + '.salesforce.com')
    
    # Connect to Salesforce
    sf = Salesforce(username=username, password=password, security_token=token, domain=domain)
    logging.debug("Connected successfully to {0}".format(sf.sf_instance))

    # initialize the csv file header row
    logging.info('Output directory: ' + output_directory)
    if not os.path.isdir(output_directory):
        os.mkdir(output_directory)
    results_path = output_directory + 'files.csv'    
    with open(results_path, 'w', encoding='UTF-8', newline='') as results_csv:
        filewriter = csv.writer(results_csv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(['FirstPublicationId','FirstPublicationName', 'ContentDocumentId', 'Title','VersionData','PathOnClient'])

    # retrieve all the ContentDocumentLinks for content documents we're going to download
    logging.info("Querying to get Content Document Ids...")
    content_document_query = 'SELECT ContentDocumentId, LinkedEntityId, LinkedEntity.Name, ContentDocument.Title, ' \
                             'ContentDocument.FileExtension FROM ContentDocumentLink ' \
                             'WHERE LinkedEntityId in ({0})'.format(args.query)    

    content_document_links = sf.query_all(content_document_query)["records"]
    logging.info("Found {0} total files".format(len(content_document_links)))

    # Begin Downloads
    global_lock = threading.Lock()
    fetch_files(sf=sf, content_document_links=content_document_links, output_directory=output_directory, results_path=results_path, batch_size=batch_size)

    
if __name__ == "__main__":
    main()
