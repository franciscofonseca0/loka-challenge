FROM clickhouse/clickhouse-server
ADD ./clickhouse_init/create_tables.sql /docker-entrypoint-initdb.d/create_tables.sql