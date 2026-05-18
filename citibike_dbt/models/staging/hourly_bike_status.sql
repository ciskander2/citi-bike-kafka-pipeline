WITH hourly_station_status AS (
    SELECT
        DATE_TRUNC('hour', ss.ingestion_time) AS hour_bucket,
        si.station_name,
        ss.num_bikes_available,
        ss.num_docks_available
    FROM station_status AS ss
    JOIN station_information AS si
        ON ss.station_id = si.station_id
),

hourly_shortages AS (
    SELECT
        hour_bucket,
        COUNT(*) AS total_station_events,
        COUNT(*) FILTER (WHERE num_bikes_available = 0) AS empty_station_events,
        COUNT(*) FILTER (WHERE num_docks_available = 0) AS full_station_events
    FROM hourly_station_status
    GROUP BY hour_bucket
)

SELECT
    hour_bucket,
    total_station_events,
    empty_station_events,
    full_station_events,
    ROUND(
        empty_station_events::NUMERIC
        / NULLIF(total_station_events, 0),
        4
    ) AS empty_station_rate,
    ROUND(
        full_station_events::NUMERIC
        / NULLIF(total_station_events, 0),
        4
    ) AS full_station_rate
FROM hourly_shortages
ORDER BY hour_bucket DESC