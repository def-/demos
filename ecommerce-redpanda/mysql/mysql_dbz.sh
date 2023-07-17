#!/usr/bin/env bash

echo "Deploying Debezium MySQL connector"

curl -s -X PUT -H  "Content-Type:application/json" http://debezium:8083/connectors/register-mysql/config \
    -d '{
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "65.109.125.29",
    "database.port": 3306,
    "database.user": "debezium",
    "database.password": "'"${MYSQL_PASSWORD}"'",
    "database.server.name": "ddnet",
    "database.server.id": "2",
    "database.allowPublicKeyRetrieval": true,
    "database.history.kafka.bootstrap.servers":"'"$KAFKA_ADDR"'",
    "database.history.kafka.topic": "mysql-history",
    "database.include.list": "teeworlds",
    "table.include.list": "teeworlds.record_race,teeworlds.record_teamrace,teeworlds.record_maps,teeworlds.record_mapinfo",
    "time.precision.mode": "connect",
    "include.schema.changes": false
 }'
