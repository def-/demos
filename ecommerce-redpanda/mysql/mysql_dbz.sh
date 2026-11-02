#!/usr/bin/env bash
: "${MYSQL_HOST:?MYSQL_HOST is unset, compose reads it from .env}"
: "${MYSQL_PASSWORD:?MYSQL_PASSWORD is unset, compose reads it from .env}"

echo "Deploying Debezium MySQL connector"

# The server name doubles as the topic prefix and as the key of the stored
# offset, so bumping it is what forces a fresh snapshot. The old topics stay
# readable meanwhile, which keeps Materialize serving while the new ones fill.
#
# Time is a MySQL FLOAT and part of the primary key. A snapshot reads it through
# JDBC, which rounds to 6 significant digits, while binlog events carry the exact
# float. Without the +0.0 the two disagree above ~10000 seconds, so every later
# UPDATE or DELETE names a row Materialize never ingested and is silently dropped,
# leaving deleted ranks in place forever.
curl -s -X PUT -H  "Content-Type:application/json" http://debezium:8083/connectors/register-mysql/config \
    -d '{
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "'"${MYSQL_HOST}"'",
    "database.port": 3306,
    "database.user": "debezium",
    "database.password": "'"${MYSQL_PASSWORD}"'",
    "database.server.name": "ddnet2",
    "database.server.id": "2",
    "database.allowPublicKeyRetrieval": true,
    "database.history.kafka.bootstrap.servers":"'"$KAFKA_ADDR"'",
    "database.history.kafka.topic": "mysql-history2",
    "database.include.list": "teeworlds",
    "table.include.list": "teeworlds.record_maps,teeworlds.record_mapinfo,teeworlds.record_mappers,teeworlds.record_teamrace,teeworlds.record_race",
    "time.precision.mode": "connect",
    "include.schema.changes": false,
    "database.sessionVariables": "max_statement_time=0",
    "snapshot.mode": "initial",
    "snapshot.select.statement.overrides": "teeworlds.record_race,teeworlds.record_teamrace",
    "snapshot.select.statement.overrides.teeworlds.record_race": "SELECT `Map`,`Name`,`Timestamp`,`Time`+0.0 AS `Time`,`Server`,`cp1`,`cp2`,`cp3`,`cp4`,`cp5`,`cp6`,`cp7`,`cp8`,`cp9`,`cp10`,`cp11`,`cp12`,`cp13`,`cp14`,`cp15`,`cp16`,`cp17`,`cp18`,`cp19`,`cp20`,`cp21`,`cp22`,`cp23`,`cp24`,`cp25`,`GameID`,`DDNet7` FROM teeworlds.record_race",
    "snapshot.select.statement.overrides.teeworlds.record_teamrace": "SELECT `Map`,`Name`,`Timestamp`,`Time`+0.0 AS `Time`,`ID`,`GameID`,`DDNet7` FROM teeworlds.record_teamrace"
 }'
