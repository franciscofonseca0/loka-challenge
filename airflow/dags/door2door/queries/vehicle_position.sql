INSERT INTO vehicle_position_data
SELECT
    date,
    vehicle_id,
    updated_at,
    lat,
    long,
    greatCircleDistance(long, lat, prev_long, prev_lat) as driven_meters
FROM
    (
        SELECT
            vehicle_id,
            lat,
            long,
            date,
            updated_at,
            if(prev_lat = 0, lat, prev_lat) as prev_lat,
            if(prev_long = 0, long, prev_long) as prev_long
        FROM
            (
                select
                    *,
                    lagInFrame(lat) over (
                        partition by vehicle_id
                        order by
                            updated_at asc rows between unbounded preceding
                            and unbounded following
                    ) AS prev_lat,
                    lagInFrame(long) OVER (
                        partition by vehicle_id
                        order by
                            updated_at asc rows between unbounded preceding
                            and unbounded following
                    ) as prev_long
                from
                    update_events
            )
    )