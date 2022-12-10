INSERT INTO
    vehicle_operation_metadata
SELECT
    ce.date AS date,
    de.vehicle_id AS vehicle_id,
    ce.operating_period_id AS operating_period_id,
    ce.start AS operation_start,
    IF(
        de.deregisted_at < ce.finish,
        de.deregisted_at,
        ce.finish
    ) AS operation_finish
FROM
    (
        SELECT
            ce.date,
            re.registed_at,
            de.deregisted_at,
            ce.operating_period_id,
            ce.start,
            ce.finish,
            de.vehicle_id
        FROM
            deregister_events de
            JOIN register_events re USING(vehicle_id)
            CROSS JOIN (
                SELECT ce.*
                FROM create_events ce
                LEFT JOIN delete_events de USING(operating_period_id)
                WHERE de.operating_period_id = ''            
            ) ce
        WHERE
            ce.date = '{date}'
            AND (
                (
                    re.registed_at BETWEEN ce.start
                    AND ce.finish
                )
                OR (
                    de.deregisted_at BETWEEN ce.start
                    AND ce.finish
                )
            )
    );