-- Sub-sample of the donor dataset

SELECT
  * 
FROM
  record_link.donors
WHERE MOD(ABS(FARM_FINGERPRINT(CAST(donor_id AS STRING))), 1000) = 0

-- Sub-sample of the contributions dataset
SELECT
  * 
FROM
  record_link.donors
WHERE MOD(ABS(FARM_FINGERPRINT(CAST(donor_id AS STRING))), 1000) = 0
