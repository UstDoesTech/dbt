-- This refers to the table created from seeds/airports.csv
select * from {{ ref('airports') }}