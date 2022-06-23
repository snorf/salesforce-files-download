# salesforce-files-download

Python script to download Salesforce Files (aka ContentDocument).
It's using ProcessPoolExecutor to run downloads in parallell which 
makes the experience nicer if you have a large number of files.

In a very non scientific test 1614 files (3.23 GB) were downloaded 
in under 6 minutes.

## Getting Started

Download the script, satisfy requirements.txt and you're good to go!

## Prerequisites

simple-salesforce (https://github.com/simple-salesforce/simple-salesforce)

## Usage

1. Copy download.ini.template to download.ini and fill it out
2. Launch the script

```
usage: download.py [-h] -q query [-o object] [-f filenamepattern]

Export ContentVersion (Files) from Salesforce

optional arguments:
  -h, --help            show this help message and exit
  -q query, --query query
                        SOQL to limit the valid ContentDocumentIds. Must
                        return the Ids of parent objects.
  -o object, --object object
                        How are the ContentDocument selected, via
                        'ContentDocumentLink' (default) or directly from
                        'ContentDocument'
  -f filenamepattern, --filenamepattern filenamepattern
                        Specify the filename pattern for the output, available
                        values are:{0} = output_directory, {1} =
                        content_document_id, {2} title, {3} file_extension,
                        Default value is: {0}{1}-{2}.{3} which will be
                        /path/ContentDocumentId-Title.fileExtension
```

## Examples
```
python download.py -q 
"SELECT Id FROM Custom_Object__c WHERE Status__c = 'Approved'"
```

You can also select directly from the ContentDocument Table and then you give the WHERE clause for the ContentDocument Query
```
python download.py -o ContentDocument 
-q "WHERE Title LIKE '1:%' AND FileExtension = 'json'  
AND Description = 'Something to filter your ContentDocuments on'"
```

## Bug free software?

This was a small implementation for a customer that I decided to clean up and put on GitHub,
I guess there are tons of bugs in here so please feel free to contact me if you find any of those.