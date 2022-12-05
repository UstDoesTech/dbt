-- All flights must have taken off and landed to be recorded as a flight
-- Therefore return records where this isn't true to make the test fail
SELECT
    TripId,
    SUM(Distance) AS TotalDistance
FROM {{ ref('gold_delays' )}}
GROUP BY 1
HAVING NOT(TotalDistance >= 0)