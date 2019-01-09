WITH
  data AS (
    SELECT
      classification.donor_id1 AS donor_id,
      GENERATE_UUID() AS cluster_id,
      ARRAY_AGG(DISTINCT classification.donor_id2) AS cluster
    FROM
      dedup.classification
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

SELECT
  cluster_id, 
  cluster_donor_id,
  donors.name,
  contributions.amount AS donation,
  contributions.date_recieved AS date,
  contributions.contribution_id
FROM
  clusters 
INNER JOIN dedup.processed_donors AS donors
ON cluster_donor_id = donors.donor_id
INNER JOIN dedup.contributions AS contributions
ON cluster_donor_id = contributions.donor_id
ORDER BY donation DESC
LIMIT 50
-- GROUP BY
--   cluster_id, donors.name
-- ORDER BY
--   donations DESC
-- LIMIT
--   50