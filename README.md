# Food-Trucks-Data-Case-Study
# Description
This is a case study of a mock food truck company. Multiple food trucks are reporting their transaction data to the cloud. This project involves the implementation of a full Extract-Transform-Load data pipeline using AWS S3, ECS, and Lambda services which are setup in code using Terraform. When run, raw data about the transactions and sales of all food trucks will be extracted from the source, cleaned and reformatted, and loaded into parquet files for long term storage. Reports will be generated using daily and emailed to configured recipients via AWS SES and Athena.
## Setup
These instructions are for MacOS. Similar commands can be found online for other operating systems.
Clone this repository
```
git clone https://github.com/SamiL52/Food-Trucks-Data-Case-Study.git
```
## Move into the desired folder
The root directory allows viewing of summary statistics and visualisations in 'analysis.ipynb'. Week 1 allows the setup of a basic pipeline to load the data as parquet files to an S3. Week 2 allows the setup of a more advanced pipeline, utilising AWS Athena to give generate email reports.

## Create and activate a virtual environment
```
python3 -m venv .venv
source .venv/bin/activate
```
## Install requirements
```
pip3 install -r requirements.txt
```
## Run
```
terraform init
terraform apply
```
## Shut down services
```
terraform destroy
```
