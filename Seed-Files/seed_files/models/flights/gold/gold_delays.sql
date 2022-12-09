SELECT 
    TripId, 
    LocaleDate,
    Delay,
    Distance,
    OriginAirport.AirportKey AS OriginAirportKey,
    DestinationAirport.airportKey AS DestinationAirportKey
FROM {{ ref('silver_delays') }}
JOIN {{ ref('gold_airports') }} AS OriginAirport ON OriginAirport.AirportCode = OriginAirport
JOIN {{ ref('gold_airports') }} AS DestinationAirport ON DestinationAirport.AirportCode = DestinationAirport