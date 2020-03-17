# salesforce-files-download

Python script to download Salesforce Files (aka ContentDocument).

## Getting Started

Download the script, satisfy requirements.txt and you're good to go!

## Prerequisites

simple-salesforce (https://github.com/simple-salesforce/simple-salesforce)

## Usage

1. Copy download.ini.template to download.ini and fill it out
2. Launch the script

```
usage: download.py [-h] -q query

Export ContentVersion (Files) from Salesforce

optional arguments:
  -h, --help            show this help message and exit
  -q query, --query query
                        SOQL to limit the valid ContentDocumentIds. Must
                        return the Id of related/parent objects.
```

## Example
```
python download.py -q 
"SELECT Id FROM Custom_Object__c WHERE Status__c = 'Approved'"
```

## Bug free software?

This was a small implementation for a customer that I decided to clean up and put on GitHub, I guess there are tons of bugs in here so please feel free to contact me if you find any of those.