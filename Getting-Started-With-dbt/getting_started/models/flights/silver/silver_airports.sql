SELECT 
    IATA AS AirportCode,
    City,
    State,
    Country
FROM {{ ref('bronze_airports') }}