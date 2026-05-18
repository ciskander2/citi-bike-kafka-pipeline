SELECT
    si.station_name,
    si.capacity,
    ss.num_bikes_available
FROM station_status AS ss
JOIN station_information AS si
    ON ss.station_id = si.station_id
GROUP BY
    si.station_name,
    si.capacity,
    ss.num_bikes_available
HAVING
    ss.num_bikes_available = 0
    AND si.capacity >= 50
ORDER BY si.capacity DESC