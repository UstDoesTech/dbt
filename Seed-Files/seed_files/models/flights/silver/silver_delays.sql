SELECT 
    CONCAT(CAST(date AS STRING),origin,destination) AS TripId, 
    CAST(concat('2014-', LEFT(date,2),'-', RIGHT(LEFT(date,4),2), ' ', LEFT(RIGHT(date,4),2), ':', RIGHT(date,2)) AS timestamp) AS LocaleDate,
    CAST(delay AS INT) AS Delay,
    CAST(distance AS INT) AS Distance,
    origin AS OriginAirport,
    destination AS DestinationAirport
FROM {{ ref('bronze_delays') }}