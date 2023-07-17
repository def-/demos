ALTER SYSTEM SET enable_disk_cluster_replicas = true;
CREATE CLUSTER disk_cluster1 REPLICAS (r1 (SIZE '1', DISK = true));
CREATE CLUSTER disk_cluster2 REPLICAS (r1 (SIZE '16', DISK = true));

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
  ENVELOPE NONE;

CREATE SOURCE record_teamrace
  IN CLUSTER disk_cluster1
  FROM KAFKA CONNECTION redpanda_connection (TOPIC 'ddnet.teeworlds.record_teamrace')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
  ENVELOPE NONE;

CREATE SOURCE record_maps
  IN CLUSTER disk_cluster1
  FROM KAFKA CONNECTION redpanda_connection (TOPIC 'ddnet.teeworlds.record_maps')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
  ENVELOPE NONE;

CREATE SOURCE record_mapinfo
  IN CLUSTER disk_cluster1
  FROM KAFKA CONNECTION redpanda_connection (TOPIC 'ddnet.teeworlds.record_mapinfo')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
  ENVELOPE NONE;

CREATE OR REPLACE VIEW race AS SELECT (after)."Map" AS map, (after)."Server" as server, (after)."Name" as name, cast((after)."Timestamp" as timestamp) as timestamp, (after)."Time" as time FROM record_race;
CREATE OR REPLACE VIEW teamrace AS SELECT (after)."Map" AS map, (after)."Name" as name, cast((after)."Timestamp" as timestamp) as timestamp, (after)."Time" as time, (after)."ID" as id, (after)."GameID" as gameid FROM record_teamrace;
CREATE OR REPLACE VIEW maps AS SELECT (after)."Map" AS map, (after)."Server" as server, (after)."Points" as points, (after)."Stars" as stars, (after)."Mapper" as mapper, cast((after)."Timestamp" as timestamp) as timestamp FROM record_maps;
CREATE INDEX maps_map IN CLUSTER disk_cluster2 ON maps (map);

--  record(Map: text,Width: integer?,Height: integer?,DEATH: integer?,THROUGH: integer?,JUMP: integer?,DFREEZE: integer?,EHOOK_START: integer?,HIT_END: integer?,SOLO_START: integer?,TELE_GUN: integer?,TELE_GRENADE: integer?,TELE_LASER: integer?,NPC_START: integer?,SUPER_START: integer?,JETPACK_START: integer?,WALLJUMP: integer?,NPH_START: integer?,WEAPON_SHOTGUN: integer?,WEAPON_GRENADE: integer?,POWERUP_NINJA: integer?,WEAPON_RIFLE: integer?,LASER_STOP: integer?,CRAZY_SHOTGUN: integer?,DRAGGER: integer?,DOOR: integer?,SWITCH_TIMED: integer?,SWITCH: integer?,STOP: integer?,THROUGH_ALL: integer?,TUNE: integer?,OLDLASER: integer?,TELEINEVIL: integer?,TELEIN: integer?,TELECHECK: integer?,TELEINWEAPON: integer?,TELEINHOOK: integer?,CHECKPOINT_FIRST: integer?,BONUS: integer?,BOOST: integer?,PLASMAF: integer?,PLASMAE: integer?,PLASMAU: integer?)
--CREATE VIEW mapinfo AS SELECT (after)."Map" AS map, (after)."Width" as width, (after)."Height" as height, (after)."Death" as death, (after)."Through" as through, (after)."Jump" as jump FROM record_maps;

-- TODO: Why is table reference l ambiguous?
-- materialize=> CREATE OR REPLACE MATERIALIZED VIEW ranks
--   IN CLUSTER disk_cluster2
--   AS SELECT l.map, l.minTime, race.timestamp, l.count, l.minTimestamp, SUBSTRING(race.server, 1, 3) FROM (
--     SELECT map, name, min(time) as minTime, name, count(*), min(timestamp) as minTimestamp,
--       ROW_NUMBER() OVER (PARTITION BY map ORDER BY min(time) ASC) as row_num
--     FROM race
--     GROUP BY map, name
--   ) l
--   JOIN race
--   ON race.map = l.map AND race.time = l.minTime and race.name = l.name
--   WHERE row_num <= 20;
-- ERROR:  table reference "l" is ambiguous
CREATE OR REPLACE MATERIALIZED VIEW ranks
  IN CLUSTER disk_cluster2
  AS SELECT l.map, l.player as name, l.minTime, race.timestamp, l.count, l.minTimestamp, SUBSTRING(race.server, 1, 3) AS server FROM (
    SELECT map, name as player, min(time) as minTime, name, count(*), min(timestamp) as minTimestamp,
      ROW_NUMBER() OVER (PARTITION BY map ORDER BY min(time) ASC) as row_num
    FROM race
    GROUP BY map, name
  ) l
  JOIN race
  ON race.map = l.map AND race.time = l.minTime and race.name = l.name
  WHERE row_num <= 20;
-- TODO: Not using index anymore
CREATE INDEX ranks_map IN CLUSTER disk_cluster2 ON ranks (map, minTime);
-- Use with: select * from ranks where map = 'Multeasymap' order by minTime;

CREATE OR REPLACE MATERIALIZED VIEW most_finishes
  IN CLUSTER disk_cluster2
  AS SELECT map, name, count, sum, min, max FROM (
    SELECT map, name, count(*), sum(time), min(timestamp), max(timestamp),
      ROW_NUMBER() OVER (PARTITION BY map ORDER BY count(*) DESC) AS row_num
    FROM race
    GROUP BY map, name
  ) WHERE row_num <= 20;
CREATE INDEX most_finishes_map IN CLUSTER disk_cluster2 ON most_finishes (map, count);

-- MariaDB: select distinct r.Name, r.ID, r.Time, r.Timestamp, (select substring(Server, 1, 3) from record_race where Map = r.Map and Name = r.Name and Time = r.Time limit 1) as Server from ((select distinct ID from record_teamrace where Map = '%s' ORDER BY Time limit 20) as l) left join record_teamrace as r on l.ID = r.ID order by r.Time, r.ID, r.Name;
CREATE OR REPLACE MATERIALIZED VIEW team_ranks
  IN CLUSTER disk_cluster2
  AS SELECT teamrace.map, name, teamrace.id, time, timestamp, (SELECT server FROM race WHERE map = teamrace.map and name = teamrace.name and time = teamrace.time limit 1) server
  FROM (
    teamrace
    JOIN
    (SELECT DISTINCT id, map, ROW_NUMBER() OVER (PARTITION BY map ORDER BY min(time)) as row_num
      FROM teamrace
      GROUP BY map, id) l
    ON l.id = teamrace.id and l.map = teamrace.map AND l.row_num <= 20);
-- TODO: Why is this index not used?
CREATE INDEX team_ranks_map IN CLUSTER disk_cluster2 ON team_ranks (map, time);
-- Use with select * from team_ranks where map = 'Multeasymap' order by time;

-- MariaDB: select (select median(Time) over (partition by Map) from record_race where Map = '%s' %s limit 1), min(Timestamp), max(Timestamp), count(*), count(distinct Name) from record_race where Map = '%s' %s
-- Doesn't support median yet: percentile_cont WITHIN GROUP in postgres, probably won't be, requires recalculation see https://materialize.com/blog/postgres-compatibility/
CREATE OR REPLACE MATERIALIZED VIEW stats
  IN CLUSTER disk_cluster2
  AS SELECT map, avg(time), min(timestamp), max(timestamp), count(*), count(distinct Name) as count_distinct
    FROM race
    GROUP BY map;
-- TODO: Why is this index not used?
CREATE INDEX stats_map IN CLUSTER disk_cluster2 ON stats (map);
-- Use: select * from stats where map = 'Multeasymap';

-- MariaDB: select count(Name) from record_teamrace where Map = '%s' group by ID order by count(Name) desc limit 1;
CREATE OR REPLACE MATERIALIZED VIEW largest_team
  IN CLUSTER disk_cluster2
  AS (SELECT map, count FROM (
        SELECT map, count(name),
          ROW_NUMBER() OVER (PARTITION BY map ORDER BY count(name) DESC) AS row_num
        FROM teamrace
        GROUP BY map, id
        ORDER BY count(name))
      WHERE row_num = 1);
CREATE INDEX largest_team_map IN CLUSTER disk_cluster2 ON largest_team (map);
-- Use: select * from largest_team where map = 'Multeasymap';

-- Now for country-specific queries:
CREATE OR REPLACE MATERIALIZED VIEW ranks_server
  IN CLUSTER disk_cluster2
  AS SELECT l.map, l.player as name, l.minTime, race.timestamp, l.count, l.minTimestamp, l.server FROM (
    SELECT server, map, name as player, min(time) as minTime, name, count(*), min(timestamp) as minTimestamp,
      ROW_NUMBER() OVER (PARTITION BY server, map ORDER BY min(time) ASC) as row_num
    FROM race
    GROUP BY server, map, name
  ) l
  JOIN race
  ON race.map = l.map AND race.time = l.minTime and race.name = l.name
  WHERE row_num <= 20;
DROP INDEX ranks_server_map;
CREATE INDEX ranks_server_map IN CLUSTER disk_cluster2 ON ranks_server (map, server, minTime);
-- Use with: select * from ranks_server where map = 'Multeasymap' and server = 'GER' order by minTime;

CREATE OR REPLACE MATERIALIZED VIEW team_ranks_server
  IN CLUSTER disk_cluster2
  AS SELECT teamrace.map, teamrace.name, teamrace.id, time, timestamp, server
  FROM teamrace
  JOIN
  (SELECT DISTINCT id, server, teamrace.map, ROW_NUMBER() OVER (PARTITION BY race.server, teamrace.map ORDER BY min(teamrace.time)) as row_num
    FROM teamrace
    JOIN race ON teamrace.map = race.map and teamrace.name = race.name and teamrace.time = race.time
    GROUP BY race.server, teamrace.map, id) l
  ON l.id = teamrace.id and l.map = teamrace.map AND l.row_num <= 20;
DROP INDEX team_ranks_server_map;
CREATE INDEX team_ranks_server_map IN CLUSTER disk_cluster2 ON team_ranks_server (map, server, time);
-- Use with select * from team_ranks_server where server = 'GER' and map = 'Multeasymap' order by time;

CREATE OR REPLACE MATERIALIZED VIEW largest_team_server
  IN CLUSTER disk_cluster2
  AS (SELECT server, map, count FROM (
        SELECT server, teamrace.map, count(teamrace.name),
          ROW_NUMBER() OVER (PARTITION BY map ORDER BY count(teamrace.name) DESC) AS row_num
        FROM teamrace
        JOIN race ON teamrace.map = race.map and teamrace.name = race.name and teamrace.time = race.time
        GROUP BY server, teamrace.map, id
        ORDER BY count(teamrace.name))
      WHERE row_num = 1);
DROP INDEX largest_team_map_server;
CREATE INDEX largest_team_map_server IN CLUSTER disk_cluster2 ON largest_team_server (map, server);
-- Use: select * from largest_team_server where server = 'GER' and map = 'Multeasymap';

CREATE OR REPLACE MATERIALIZED VIEW most_finishes_server
  IN CLUSTER disk_cluster2
  AS SELECT server, map, name, count, sum, min, max FROM (
    SELECT server, map, name, count(*), sum(time), min(timestamp), max(timestamp),
      ROW_NUMBER() OVER (PARTITION BY map ORDER BY count(*) DESC) AS row_num
    FROM race
    GROUP BY server, map, name
  ) WHERE row_num <= 20;
DROP INDEX most_finishes_server_map;
CREATE INDEX most_finishes_server_map IN CLUSTER disk_cluster2 ON most_finishes_server (map, server, count);

CREATE OR REPLACE MATERIALIZED VIEW stats_server
  IN CLUSTER disk_cluster2
  AS SELECT server, map, avg(time), min(timestamp), max(timestamp), count(*), count(distinct Name) as count_distinct
    FROM race
    GROUP BY server, map;
DROP INDEX stats_server_map;
CREATE INDEX stats_server_map IN CLUSTER disk_cluster2 ON stats_server (map, server);
