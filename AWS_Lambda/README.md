## Description

This folder contain AWS Lambda docker containers. The instructions on AWS ECR should be followed to build and push
each container into ECR repository to be used for AWS Lambda functions.

## Getting Started

### Dependencies

* Docker Desktop

### Executing program

* Navigate to each individual folder with docker and follow instructions on AWS ECR to upload the docker image

## File structure

```
├── 
├── downloading             # Contains extraction ETL logic
│   ├── blocks              # Download blocks dump file from blockchair
│   └── transactions        # Download transactions dump file from blockchair
└── transformating          # Contains transformation ETL logic
    ├── blocks              # Compute metrics for block dump files
    ├── charts              # Compute metrics for charts data
    ├── combine             # Combine all computed metrics
    └── transactions        # Compute metrics for transaction dump files
```

## To define a config file for the lambda function

Create a `config.json` file (replace the credentials keys, password and file path with your own):
```
{
    "ACCESS_KEY": YOUR_AWS_ACCESS_KEY
    "SECRET_KEY": YOUR_AWS_SECRET_KEY
    "BLOCKCHAIR_KEY": YOUR_BLOCKCHAIR_API_KEY
    "DATA_TYPE": CHOICE OF BLOCKCHAIR DUMP FILE DATA TO DOWNLOAD
    "NUM_FILES": NUMBER OF FILES TO DOWNLOAD PER LAMBDA FUNCTION CALL
}
```

## Acknowledgments

Xeptagon 