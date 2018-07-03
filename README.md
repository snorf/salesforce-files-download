# salesforce-files-download

Python script to download Salesforce Files (aka ContentDocument).

## Getting Started

Download the script, install simple_salesforce and you're good to go!

## Prerequisites

simple-salesforce (https://github.com/simple-salesforce/simple-salesforce)

## Usage

```
usage: download.py [-h] -u username -p password -t security_token
                   [-sb sandbox] -q query [-cdq cdqquery] [-o OUTPUT]

Export ContentVersion (Files) from Salesforce

optional arguments:
  -h, --help            show this help message and exit
  -u username, --user username
                        Your Salesforce username
  -p password, --password password
                        Your Salesforce password
  -t security_token, --token security_token
                        Your Security Token
  -sb sandbox, --sandbox sandbox
                        is this a sandbox instance?
  -q query, --query query
                        SOQL query (has to contain Title and VersionData and
                        SELECT FROM ContentVersion)
  -cdq cdqquery, --content_document_query cdqquery
                        SOQL to limit the valid ContentDocumentIds, if this is
                        set you need ContentDocumentId in your ContentVersion
                        query
  -o OUTPUT, --output OUTPUT
                        Specify output directory
```

## Examples
```
python download.pi
-u
salesforce@username.com
-p
PASSWORD
-t
SECURITY_TOKEN
-q
"SELECT ContentDocumentId, Title, VersionData, CreatedDate FROM ContentVersion WHERE IsLatest = True AND Title LIKE '%Example%' ORDER BY Title DESC, CreatedDate DESC"
-o
output_directory
```

You can filter more by adding a Content Document query like this

```
python download.pi
-u
salesforce@username.com
-p
PASSWORD
-t
SECURITY_TOKEN
-q
"SELECT ContentDocumentId, Title, VersionData, CreatedDate FROM ContentVersion WHERE IsLatest = True AND Title LIKE '%Example%' ORDER BY Title DESC, CreatedDate DESC"
-cdq
"SELECT ContentDocumentId  FROM ContentDocumentLink where LinkedEntityId IN (SELECT Id FROM Custom_Object__c WHERE Status__c = 'Approved')"
-o
output_directory
```

## Bug free software?

This was a small implementation for a customer that I decided to clean up and put on GitHub, I guess there are tons of bugs in here so please feel free to contact me if you find any of those.