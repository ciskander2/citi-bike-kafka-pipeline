WITH station_data AS (
    SELECT
        si.station_name,
        si.capacity,
        ss.num_bikes_available,
        ss.num_docks_available
    FROM station_status AS ss
    JOIN station_information AS si
        ON ss.station_id = si.station_id
)

SELECT
    station_name,
    MAX(num_bikes_available) AS max_num_bikes_available,
    MAX(num_docks_available) AS max_num_docks_available
FROM station_data
WHERE
    capacity >= 50
    AND num_docks_available = 0
GROUP BY station_name
ORDER BY max_num_bikes_available DESC