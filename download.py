from simple_salesforce import Salesforce
import requests
import os.path


def get_content_document_ids(sf, query):
    content_document_ids = set()
    content_documents = sf.query_all(query)
    for content_document in content_documents["records"]:
        content_document_ids.add(content_document["ContentDocumentId"])
    return content_document_ids


def fetch_files(sf, query_string, output_directory, valid_content_document_ids=None):
    query_response = sf.query(query_string)

    files = 0
    already_downloaded = 0
    existing_filenames = {}
    if not os.path.isdir(output_directory):
        os.mkdir(output_directory)
    while query_response:
        for r in query_response["records"]:
            content_document_id = r["ContentDocumentId"]
            title = r["Title"]

            filename = "%s/%s" % (output_directory, title)
            url = "https://%s%s" % (sf.sf_instance, r["VersionData"])
            created_date = r["CreatedDate"]
            if valid_content_document_ids and content_document_id not in valid_content_document_ids:
                print "Ignoring (%s) %s" % (content_document_id, title)
                continue
            if not os.path.isfile(filename):
                response = requests.get(url, headers={"Authorization": "OAuth " + sf.session_id,
                                                      "Content-Type": "application/octet-stream"})
                if response.ok:
                    print "Saving %s" % title
                    with open(filename, "wb") as output_file:
                        output_file.write(response.content)
                    existing_filenames[title] = created_date
                    files += 1
                else:
                    print "Couldn't download %s" % title
            else:
                if title not in existing_filenames:
                    existing_filenames[title] = created_date
                else:
                    print "%s (%s) already in list (%s)" % (title, existing_filenames[title], created_date)
                already_downloaded += 1
        if "nextRecordsUrl" in query_response:
            next_records_identifier = query_response["nextRecordsUrl"]
            query_response = sf.query_more(next_records_identifier=next_records_identifier, identifier_is_url=True)
        else:
            print "%d files downloaded" % files
            print "%d already downloaded" % already_downloaded
            break


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Export ContentVersion (Files) from Salesforce')
    parser.add_argument('-u', '--user', metavar='username', required=True,
                        help='Your Salesforce username')
    parser.add_argument('-p', '--password', metavar='password', required=True,
                        help='Your Salesforce password')
    parser.add_argument('-t', '--token', metavar='security_token', required=True,
                        help='Your Security Token')
    parser.add_argument('-sb', '--sandbox', metavar='sandbox', required=False, default='True',
                        help='is this a sandbox instance?')
    parser.add_argument('-q', '--query', metavar='query', required=True,
                        help='SOQL query (has to contain Title and VersionData and SELECT FROM ContentVersion)')
    parser.add_argument('-cdq', '--content_document_query', metavar='cdqquery', required=False,
                        help='SOQL to limit the valid ContentDocumentIds, if this is set you need ContentDocumentId '
                             'in your ContentVersion query')
    parser.add_argument('-o', '--output', required=False, default='output',
                        help='Specify output directory')
    args = parser.parse_args()

    domain = None
    if args.sandbox == 'True':
        domain = 'test'

    sf = Salesforce(username=args.user, password=args.password,
                    security_token=args.token, domain=domain)

    valid_content_document_ids = None
    if args.content_document_query:
        valid_content_document_ids = get_content_document_ids(sf=sf, query=args.content_document_query)

    fetch_files(sf=sf, query_string=args.query, valid_content_document_ids=valid_content_document_ids, output_directory=args.output)


if __name__ == "__main__":
    main()
