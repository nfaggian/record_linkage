#! /bin/bash

# GCP host project
PROJECT="anz-pso-nfaggian"
# Temporary GCS bucket
BUCKET="gs://anz-pso-nfaggian-dedup-beam/tmp/"
# Output BigQuery table
BQ_TABLE="anz-pso-nfaggian:dedup.classification"

# Execute the python pipeline
python record_link.py --output ${BQ_TABLE} \
                      --runner DataflowRunner \
                      --project ${PROJECT} \
                      --temp_location $BUCKET \
                      --requirements_file requirements.txt