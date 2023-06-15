-- (1)Find the average temperature recorded for each device.
select device_id,avg(temperature) from cleaned_environment
group by device_id;


-- (2)Retrieve the top 5 devices with the highest average carbon monoxide levels.
select device_id,avg(carbon_monoxide) from cleaned_environment
group by device_id
order by carbon_monoxide desc
limit 5;

-- (3)Calculate the average temperature recorded in the cleaned_environment table.
select avg(temperature) from cleaned_environment ;

-- (4)Find the timestamp and temperature of the highest recorded temperature for each device.
select device_id,timestamp,max(temperature) from cleaned_environment
group by device_id limit 3;

-- (5) Identify devices where the temperature has increased from the minimum recorded temperature to the maximum recorded temperature.
SELECT device_id
FROM cleaned_environment
GROUP BY device_id
HAVING MAX(temperature) > MIN(temperature);

-- (6) Calculate the exponential moving average of temperature for each device limit to 10 devices.

SELECT device_id, timestamp, temperature, ema_temperature 
FROM ( SELECT device_id, timestamp, temperature, AVG(temperature) 
OVER (PARTITION BY device_id ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ema_temperature,
ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY timestamp) AS row_num FROM cleaned_environment ) subquery
WHERE row_num <= 10 ORDER BY device_id, timestamp LIMIT 10;

-- (7) Find the timestamps and devices where carbon monoxide level exceeds the average carbon monoxide level of all devices.
SELECT timestamp,device_id
FROM cleaned_environment
WHERE carbon_monoxide>(SELECT AVG(carbon_monoxide) FROM cleaned_environment);

-- (8) Retrieve the devices with the highest average temperature recorded.
select device_id,avg(temperature) as temp from cleaned_environment
group by device_id
order by temp desc;

-- (9) Calculate the average temperature for each hour of the day across all devices.
SELECT EXTRACT(HOUR FROM timestamp) AS hour_of_day, AVG(temperature) AS average_temperature
FROM cleaned_environment
GROUP BY EXTRACT(HOUR FROM timestamp)
ORDER BY hour_of_day;

-- (10) Which device(s) in the cleaned environment dataset have recorded only a single distinct temperature value?.
SELECT device_id from
cleaned_environment
GROUP BY device_id
HAVING count(DISTINCT temperature)=1;

-- (11) Find the devices with the highest humidity levels.
select device_id,max(humidity) as hum from cleaned_environment
group by device_id
order by hum desc;

-- (12) Calculate the average temperature for each device, excluding outliers (temperatures beyond 3 standard deviations).
SELECT device_id, AVG(temperature) AS average_temperature 
FROM cleaned_environment 
WHERE temperature BETWEEN (SELECT AVG(temperature) - 3 * STDDEV(temperature) FROM cleaned_environment) AND (SELECT AVG(temperature) + 3 * STDDEV(temperature) 
FROM cleaned_environment) GROUP BY device_id;

-- (13) Retrieve the devices that have experienced a sudden change in humidity (greater than 50% difference) within a 30-minute window.

SELECT table1.device_id, table1.timestamp, table1.humidity
FROM
(SELECT device_id, timestamp,
humidity,
LAG(humidity,1) OVER (
PARTITION BY device_id
ORDER BY timestamp),
(humidity - (LAG(humidity,1) OVER (
PARTITION BY device_id
ORDER BY timestamp))) diff,
ABS((humidity - (LAG(humidity,1) OVER (
PARTITION BY device_id
ORDER BY timestamp)))*100) c1
FROM `cleaned_environment`) table1
WHERE table1.c1 > 50;

-- (14) Find the average temperature for each device during weekdays and weekends separately.
SELECT device_id,
CASE WHEN DAYOFWEEK(timestamp) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
AVG(temperature) AS average_temperature
FROM cleaned_environment
GROUP BY device_id, day_type;

-- (15) Calculate the cumulative sum of temperature for each device, ordered by timestamp limit to 10.

SELECT device_id, timestamp, temperature, SUM(temperature) 
OVER (PARTITION BY device_id ORDER BY timestamp) AS cumulative_temperature
FROM cleaned_environment LIMIT 10;

