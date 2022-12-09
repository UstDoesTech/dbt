SELECT 
    monotonically_increasing_id() AS AirportKey,
    AirportCode,
    City,
    State,
    Country
FROM {{ ref('silver_airports') }}