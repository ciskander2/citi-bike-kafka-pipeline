SELECT
    si.station_name,
    ss.num_bikes_available::FLOAT
        / CASE
            WHEN (ss.num_bikes_available + ss.num_docks_available) = 0 THEN NULL
            ELSE (ss.num_bikes_available + ss.num_docks_available)
          END AS bike_utilization_ratio
FROM station_status AS ss
JOIN station_information AS si
    ON si.station_id = ss.station_id
ORDER BY bike_utilization_ratio DESC NULLS LAST
