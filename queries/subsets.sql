-- Sample 1/1000th of the donor dataset

SELECT
  * 
FROM
  dedup.donors
WHERE MOD(ABS(FARM_FINGERPRINT(CAST(donor_id AS STRING))), 1000) = 0

-- Sample 1/1000th of the contributions dataset
SELECT
  * 
FROM
  dedup.donors
WHERE MOD(ABS(FARM_FINGERPRINT(CAST(donor_id AS STRING))), 1000) = 0
