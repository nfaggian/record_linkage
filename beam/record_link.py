#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
python record_link.py --output anz-pso-nfaggian:dedup.classification \
                      --runner DataflowRunner \
                      --project anz-pso-nfaggian \
                      --temp_location gs://anz-pso-nfaggian-dedup-beam/tmp/ \
                      --requirements_file requirements.txt
"""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

import numpy as np
import jellyfish as jf   


BLOCKING_QUERY = """
WITH
  name_index AS (
   -- name: sorted neighbourhood indexing method 
  SELECT
    donor_id,
    name,
    address,
    ARRAY_AGG(STRUCT(donor_id, name, address)) OVER (ROWS BETWEEN 20 PRECEDING AND 20 FOLLOWING) AS name_candidates
  FROM (
    SELECT
      *
    FROM
      dedup.processed_donors
    ORDER BY
      name) ),
  address_index AS (
  -- address: sorted neighbourhood indexing method
  SELECT
    donor_id,
    name,
    address,
    ARRAY_AGG(STRUCT(donor_id, name, address)) OVER (ROWS BETWEEN 20 PRECEDING AND 20 FOLLOWING) AS address_candidates
  FROM (
    SELECT
      *
    FROM
      dedup.processed_donors
    ORDER BY
      address) )
SELECT
  name_index.donor_id,
  name_index.name,
  name_index.name_candidates,
  address_index.address,
  address_index.address_candidates
FROM
  address_index
JOIN
  name_index
ON
  address_index.donor_id = name_index.donor_id
"""

class indexer(beam.DoFn): 
    """
    Forms candidate pairs from a structured query
    """
    def process(self, element):
        """
        Split the candidates
        """    
        candidate_groups = ['address_candidates', 'name_candidates']
        for group in candidate_groups:
            for candidate in element[group]:
                yield {'record_a': {'donor_id': element['donor_id'], 
                                    'name': unicode(element['name']), 
                                    'address': unicode(element['address'])},
                       'record_b': {'donor_id': candidate['donor_id'], 
                                    'name': unicode(candidate['name']), 
                                    'address': unicode(candidate['address'])}}
                
def comparator(element):
    """
    Extract similarity features
    """
    
    return {
        'donor_id1': element['record_a']['donor_id'],
        'donor_id2': element['record_b']['donor_id'],
        'jaro_name': jf.jaro_winkler(element['record_a']['name'], element['record_b']['name']),
        'damerau_name': jf.damerau_levenshtein_distance(element['record_a']['name'], element['record_b']['name']),
        'jaro_address': jf.jaro_winkler(element['record_a']['address'], element['record_b']['address']),
        'damerau_address': jf.damerau_levenshtein_distance(element['record_a']['address'], element['record_b']['address'])  
        }
                
def baseline_classifier(element):
    """
    Simple voting classifier.
    * assumes an equal weighting for the different types of distance metrics. 
    """
    
    votes = [
        element['jaro_name'] > 0.67,
        element['jaro_address'] > 0.67,
        element['damerau_name'] < 9,
        element['damerau_address'] < 9]
    return {'donor_id1': str(element['donor_id1']), 
            'donor_id2': str(element['donor_id2']), 
            'classification': np.mean(votes)}          


def run(argv=None):
    """The main function which creates the pipeline and runs it."""
    parser = argparse.ArgumentParser()
  
    parser.add_argument('--output', dest='output', required=False,
                        help='Output BQ table to write results to.',
                        default='dedup.classification')
    
    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)
    print(known_args)
    
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    with beam.Pipeline(options=pipeline_options) as p:

        _ = (p 
            | "query" >> beam.io.Read(beam.io.BigQuerySource(query=BLOCKING_QUERY, 
                                                             #project=known_args.project, 
                                                             use_standard_sql=True))
            | "record generator" >> beam.ParDo(indexer())
            | "feature extraction" >> beam.Map(lambda x: comparator(x)) 
            | "duplicate classifier" >> beam.Map(lambda x: baseline_classifier(x)) 
            # Need a cluster creation function here 
            | "store" >> beam.io.Write(beam.io.BigQuerySink(known_args.output, 
                                                            schema='donor_id1:STRING, donor_id2:STRING, classification:FLOAT', 
                                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
            )

        
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()