
-- Find avg fair amount based on each vendorid

SELECT VendorID, AVG(fare_amount) AS avg_fare
FROM `uber-data-engineering-project2.uber_dataset_urmi.fact_table`
GROUP BY VendorID;

-- SUM tip_amount given based on payment type

SELECT b.payment_type_name, SUM(a.tip_amount) 
FROM `uber-data-engineering-project2.uber_dataset_urmi.fact_table` a
JOIN `uber-data-engineering-project2.uber_dataset_urmi.payment_type_dim` b 
ON a.payment_type_id = b.payment_type_id
GROUP BY b.payment_type_name
;

-- Find top 10 pickup locations based on the number of trips

SELECT 
f.pickup_location_id,
p.pickup_longitude,
p.pickup_longitude,
COUNT(*) AS total_trips
FROM `uber-data-engineering-project2.uber_dataset_urmi.fact_table` f
JOIN `uber-data-engineering-project2.uber_dataset_urmi.pickup_location_dim` p
ON f.pickup_location_id = p.pickup_location_id
GROUP BY 
f.pickup_location_id,
p.pickup_longitude,
p.pickup_longitude
ORDER BY total_trips DESC
LIMIT 10;

-- find total numbers of trips by passenger count
SELECT 
p.passenger_count,
COUNT(*) AS total_num_of_trips
FROM `uber-data-engineering-project2.uber_dataset_urmi.fact_table`f
jOIN `uber-data-engineering-project2.uber_dataset_urmi.passenger_count_dim` p
ON p.passenger_count_id = f.passenger_count_id
GROUP BY 
p.passenger_count
ORDER BY p.passenger_count DESC;

-- find average fare amount by hour of the day

SELECT 
d.pickup_hour,
AVG(f.fare_amount),
FROM `uber-data-engineering-project2.uber_dataset_urmi.fact_table` f
JOIN `uber-data-engineering-project2.uber_dataset_urmi.datetime_dim` d
ON f.datetime_id = d.datetime_id
GROUP BY d.pickup_hour 
order by d.pickup_hour DESC;
















