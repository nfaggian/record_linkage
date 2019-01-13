WITH
  data AS (
    SELECT
      classification.donor_id1 AS donor_id,
      GENERATE_UUID() AS cluster_id,
      ARRAY_AGG(DISTINCT classification.donor_id2) AS cluster
    FROM
      record_link.classification
    WHERE
      classification.classification > 0.75
    GROUP BY
      classification.donor_id1
      ),
  clusters AS (
    SELECT
      CAST(cluster_donor_id AS INT64) AS cluster_donor_id,
      cluster_id
    FROM
      data
    CROSS JOIN
      UNNEST (cluster) AS cluster_donor_id
    )
