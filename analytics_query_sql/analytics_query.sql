SELECT 
f.VendorID,
d.tpep_pickup_datetime,
d.tpep_dropoff_datetime,
p.passenger_count,
t.trip_distance,
r.ratecode_name,
pu.pickup_latitude,
pu.pickup_longitude,
d1.dropoff_latitude,
d1.dropoff_longitude,
pt.payment_type_name,
f.fare_amount,
f.mta_tax,
f.tip_amount,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount,



FROM


`uber-data-engineering-project2.uber_dataset_urmi.fact_table` f    -- done
JOIN `uber-data-engineering-project2.uber_dataset_urmi.datetime_dim` d --d
ON f.datetime_id = d.datetime_id

JOIN `uber-data-engineering-project2.uber_dataset_urmi.passenger_count_dim` p  --d  
ON f.passenger_count_id = p.passenger_count_id

JOIN `uber-data-engineering-project2.uber_dataset_urmi.trip_distance_dim` t  --d
ON f.trip_distance_id = t.trip_distance_id

JOIN `uber-data-engineering-project2.uber_dataset_urmi.pickup_location_dim` pu  --d
ON f.pickup_location_id = pu.pickup_location_id

JOIN `uber-data-engineering-project2.uber_dataset_urmi.dropoff_location_dim` d1 --d
ON f.dropoff_location_id = d1.dropoff_location_id

JOIN `uber-data-engineering-project2.uber_dataset_urmi.ratecode_dim` r   --d
ON f.RatecodeID = r.rate_code_id

JOIN `uber-data-engineering-project2.uber_dataset_urmi.payment_type_dim` pt
ON f.payment_type_id = pt.payment_type_id

