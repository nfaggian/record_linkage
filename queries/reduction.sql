-- Example of applying the neighbourhood indexing method to reduce computation time
-- Refer to: http://networkslab.org/2017/07/18/2017-07-18-neighbourhood/
WITH
  name_index AS (
   -- name: sorted neighbourhood indexing method 
  SELECT
    donor_id,
    name,
    ARRAY_AGG(STRUCT(donor_id, name)) OVER (ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS name_candidates
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
    address,
    ARRAY_AGG(STRUCT(donor_id, address)) OVER (ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS address_candidates
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
