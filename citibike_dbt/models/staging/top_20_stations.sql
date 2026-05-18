WITH station_max_ebikes AS (
    SELECT
        si.station_name,
        si.capacity,
        MAX(ss.num_ebikes_available) AS max_ebikes_available
    FROM station_status AS ss
    JOIN station_information AS si
        ON ss.station_id = si.station_id
    GROUP BY si.station_name, si.capacity
),
ranked_stations AS (
    SELECT
        station_name,
        capacity,
        max_ebikes_available,
        RANK() OVER (ORDER BY max_ebikes_available DESC) AS ebike_rank
    FROM station_max_ebikes
)

SELECT
    station_name,
    capacity,
    max_ebikes_available,
    ebike_rank
FROM ranked_stations
ORDER BY ebike_rank
LIMIT 20