ALTER SYSTEM SET enable_disk_cluster_replicas = true

CREATE CLUSTER disk_cluster1 REPLICAS (r1 (SIZE '1', DISK = true));

CREATE CLUSTER disk_cluster2 REPLICAS (r1 (SIZE '1', DISK = true));

DROP CONNECTION IF EXISTS redpanda_connection CASCADE;
DROP CONNECTION IF EXISTS schema_registry CASCADE;

CREATE CONNECTION redpanda_connection
  TO KAFKA (BROKER '127.0.0.1:9092');

CREATE CONNECTION schema_registry
  TO CONFLUENT SCHEMA REGISTRY (URL 'http://127.0.0.1:8081');

CREATE SOURCE record_race
  IN CLUSTER disk_cluster1
  FROM KAFKA CONNECTION redpanda_connection (TOPIC 'ddnet.teeworlds.record_race')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
  ENVELOPE DEBEZIUM;

CREATE SOURCE record_teamrace
  IN CLUSTER disk_cluster1
  FROM KAFKA CONNECTION redpanda_connection (TOPIC 'ddnet.teeworlds.record_teamrace')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
  ENVELOPE DEBEZIUM;

CREATE SOURCE record_maps
  IN CLUSTER disk_cluster1
  FROM KAFKA CONNECTION redpanda_connection (TOPIC 'ddnet.teeworlds.record_maps')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
  ENVELOPE DEBEZIUM;

CREATE SOURCE record_mapinfo
  IN CLUSTER disk_cluster1
  FROM KAFKA CONNECTION redpanda_connection (TOPIC 'ddnet.teeworlds.record_mapinfo')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
  ENVELOPE DEBEZIUM;

SET CLUSTER = disk_cluster2;

CREATE DEFAULT INDEX ON record_race;
CREATE DEFAULT INDEX ON record_teamrace;
CREATE DEFAULT INDEX ON record_maps;
CREATE DEFAULT INDEX ON record_mapinfo;

-- Fails to work for relevant queries, default index is fine:
--CREATE INDEX race_timestamp ON record_race ("Timestamp");
--CREATE INDEX race_timestamp_server ON record_race ("Timestamp", "Server");

-- MariaDB: select Name, count(*), sum(Time), min(Timestamp), max(Timestamp) from record_race where Map = '%s' %s group by Name order by count(*) desc limit 20;
CREATE OR REPLACE MATERIALIZED VIEW most_finishes AS
  SELECT * FROM (
    SELECT "Map", count(*), "Name", sum("Time"), min("Timestamp"), max("Timestamp"),
      ROW_NUMBER() OVER (PARTITION BY "Map" ORDER BY count(*) DESC) AS row_num
    FROM record_race
    GROUP BY "Map", "Name"
  ) WHERE row_num <= 20;
CREATE INDEX most_finishes_map ON most_finishes ("Map", count);
-- Use with: select * from most_finishes where "Map" = 'Multeasymap' order by count desc;

-- MariaDB: select l.Name, minTime, l.Timestamp, playCount, minTimestamp, SUBSTRING(l.Server, 1, 3) from (select * from record_race where Map = '%s' %s) as l JOIN (select Name, min(Time) as minTime, count(*) as playCount, min(Timestamp) as minTimestamp from record_race where Map = '%s' %s group by Name order by minTime ASC limit 20) as r on l.Time = r.minTime and l.Name = r.Name GROUP BY Name ORDER BY minTime, l.Name;
CREATE OR REPLACE MATERIALIZED VIEW ranks AS
  SELECT * FROM (
    SELECT "Map", min("Time") as minTime, "Name", count(*), min("Timestamp") as minTimestamp,
      ROW_NUMBER() OVER (PARTITION BY "Map" ORDER BY min("Time") ASC) as row_num
    FROM record_race
    GROUP BY "Map", "Name"
  ) WHERE row_num <= 20;
CREATE INDEX ranks_map ON ranks ("Map", minTime);
-- Use with: select * from ranks where "Map" = 'Multeasymap' order by minTime;

-- Try normalizing instead:
CREATE OR REPLACE MATERIALIZED VIEW maps AS SELECT row_number() OVER (ORDER BY "Timestamp", "Map") as id, "Map" as map FROM record_maps;
CREATE DEFAULT INDEX ON maps;
-- Use with: select map from maps where id = 2204;

CREATE OR REPLACE MATERIALIZED VIEW players AS SELECT row_number() OVER (ORDER BY min("Timestamp"), "Name") as id, "Name" as name FROM record_race GROUP BY name;
CREATE DEFAULT INDEX ON players;
-- Use with: select name from players where id = 2204;

CREATE OR REPLACE MATERIALIZED VIEW ranks AS
  SELECT * FROM (
    SELECT maps.id as map_id, min("Time") as min_time, players.id as player_id, count(*), min("Timestamp") as min_timestamp,
      ROW_NUMBER() OVER (PARTITION BY maps.id ORDER BY min("Time") ASC) as row_num
    FROM record_race
    JOIN maps ON maps.map = record_race."Map"
    JOIN players ON players.name = record_race."Name"
    GROUP BY maps.id, players.id
  ) WHERE row_num <= 20;
CREATE INDEX ranks_map ON ranks (map_id, min_time);
-- Use with: select map, row_num, name, min_time, count, min_timestamp from ranks JOIN maps ON ranks.map_id = maps.id JOIN players ON ranks.player_id = players.id where map = 'Multeasymap' order by row_num;
