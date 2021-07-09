SELECT
    count(*) count,
    coalesce(vendorid,-1) vendorid,
    day,
    month,
    year,
    pulocationid,
    dolocationid,
    payment_type,
    sum(passenger_count) passenger_count,
    sum(trip_distance) total_trip_distance,
    sum(fare_amount) total_fare_amount,
    sum(extra) total_extra,
    sum(tip_amount) total_tip_amount,
    sum(tolls_amount) total_tolls_amount,
    sum(total_amount) total_amount
FROM tlc_taxi_data.green_taxi_record_data
GROUP BY vendorid, day, month, year, day, month, year, pulocationid, dolocationid, payment_type
