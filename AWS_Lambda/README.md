## Description

An in-depth paragraph about your project and overview of use.

## Getting Started

### Dependencies

* Docker Desktop

### Executing program

* Navigate to each individual folder with docker and follow instructions on AWS ECR to upload the docker image

## File structure

.
```
├── ...
```
├── downloading             # Contains 
│   ├── benchmarks          # Load and stress tests
│   ├── integration         # End-to-end, integration tests (alternatively `e2e`)
│   └── unit                # Unit tests
└── transformation

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

## Acknowledgments

Xeptagon 