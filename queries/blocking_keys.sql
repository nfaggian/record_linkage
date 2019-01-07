-- Example of applying the neighbourhood indexing method to reduce computation time
-- Refer to: http://networkslab.org/2017/07/18/2017-07-18-neighbourhood/
WITH 
name_candidates AS (
  SELECT
    donor_id,
    name,
    -- window component
    ARRAY_AGG(STRUCT(donor_id, name)) OVER (ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS candidates
  FROM 
  (
  -- Sort component
    SELECT
      donor_id, 
      name
    FROM
      dedup.processed_donors
    WHERE name IS NOT NULL
    ORDER BY name)
    ),
address_candidates AS (
  SELECT
    donor_id,
    address,
    -- window component
    ARRAY_AGG(STRUCT(donor_id, address)) OVER (ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS candidates
  FROM 
  (
  -- Sort component
    SELECT
      donor_id, 
      address
    FROM
      dedup.processed_donors
    WHERE address IS NOT NULL
    ORDER BY address)
)
SELECT  
  name_candidates.donor_id, 
  name_candidates.name, 
  address_candidates.address, 
  ARRAY_CONCAT(address_candidates.candidates, name_candidates.candidates)
FROM
  name_candidates
JOIN
  address_candidates
ON
  address_candidates.donor_id = name_candidates.donor_id
