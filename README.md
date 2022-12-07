# DB Importer
Suite of scripts to upload compressed files into a database (WIP)

## TODO
- [ ] Finish script to dump a few lines from every file
- [ ] Finish tests for dump script
- [ ] Write script for database upload
- [ ] Write tests for database upload script
- [ ] Add detailed guide on how to use the script suite

## Requirements
- Python3
- Elasticsearch

## Installation
1. Clone the repo
2. `cd` inside the cloned repo
3. Run `pip3 install -r requirements.txt`
    * Linux: you will need to install `libmagic1` with your package manager
    * macOS: you will need to install `libmagic` (if using brew) or `file` (if using macports)
    * Windows: `requirements.txt` automatically installs the required binaries (`python-magic-bin`)


## Usage
- Create a `.env` file in the base of the cloned repo and add the following values:
    ```
    ES_HOST=<elasticsearch url>
    ES_USER=<elasticsearch user>
    ES_PASS=<elasticsearch password>
    DATA_PATH=<path to data directory>
    SCHEMA_PATH=<path to write schemas>
    ```
- TODO