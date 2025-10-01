DROP CLUSTER IF EXISTS source_cluster CASCADE;
DROP CLUSTER IF EXISTS compute_cluster CASCADE;
DROP CLUSTER IF EXISTS serving_cluster CASCADE;
CREATE CLUSTER source_cluster (SIZE '100cc');
CREATE CLUSTER compute_cluster (SIZE '800cc');
CREATE CLUSTER serving_cluster (SIZE '100cc');

DROP CONNECTION IF EXISTS redpanda_connection CASCADE;
DROP CONNECTION IF EXISTS schema_registry CASCADE;

CREATE CONNECTION redpanda_connection
  TO KAFKA (BROKER '127.0.0.1:9092', SECURITY PROTOCOL = 'PLAINTEXT');

CREATE CONNECTION schema_registry
  TO CONFLUENT SCHEMA REGISTRY (URL 'http://127.0.0.1:8081');

CREATE SOURCE record_race
  IN CLUSTER source_cluster
  FROM KAFKA CONNECTION redpanda_connection (TOPIC 'ddnet.teeworlds.record_race')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
  ENVELOPE DEBEZIUM;

CREATE SOURCE record_teamrace
  IN CLUSTER source_cluster
  FROM KAFKA CONNECTION redpanda_connection (TOPIC 'ddnet.teeworlds.record_teamrace')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
  ENVELOPE DEBEZIUM;

CREATE SOURCE record_maps
  IN CLUSTER source_cluster
  FROM KAFKA CONNECTION redpanda_connection (TOPIC 'ddnet.teeworlds.record_maps')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
  ENVELOPE DEBEZIUM;

CREATE SOURCE record_mapinfo
  IN CLUSTER source_cluster
  FROM KAFKA CONNECTION redpanda_connection (TOPIC 'ddnet.teeworlds.record_mapinfo')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
  ENVELOPE DEBEZIUM;

CREATE SOURCE record_mappers
  IN CLUSTER source_cluster
  FROM KAFKA CONNECTION redpanda_connection (TOPIC 'ddnet.teeworlds.record_mappers')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
  ENVELOPE DEBEZIUM;

SET cluster = compute_cluster;

CREATE OR REPLACE VIEW race AS SELECT "Map" AS map, "Server" as server, "Name" as name, cast("Timestamp" as timestamp) as timestamp, "Time" as time FROM record_race;
CREATE OR REPLACE VIEW teamrace AS SELECT "Map" AS map, "Name" as name, cast("Timestamp" as timestamp) as timestamp, "Time" as time, "ID" as id, "GameID" as gameid FROM record_teamrace;
CREATE OR REPLACE VIEW maps AS SELECT "Map" AS map, "Server" as server, "Points" as points, "Stars" as stars, "Mapper" as mapper, cast("Timestamp" as timestamp) as timestamp FROM record_maps;

CREATE OR REPLACE VIEW mappers AS SELECT "Mapper" AS Mapper, "NumMaps" as nummaps FROM record_mappers;
CREATE VIEW mapinfo AS SELECT "Map" AS map, "Width" as width, "Height" as height, "DEATH" as death, "THROUGH" as through, "JUMP" as jump, "DFREEZE" AS dfreeze, "EHOOK_START" AS ehook_start, "HIT_END" AS hit_end, "SOLO_START" AS solo_start, "TELE_GUN" AS tele_gun, "TELE_GRENADE" AS tele_grenade, "TELE_LASER" AS tele_laser, "NPC_START" AS npc_start, "SUPER_START" AS super_start, "JETPACK_START" AS jetpack_start, "WALLJUMP" AS walljump, "NPH_START" AS nph_start, "WEAPON_SHOTGUN" AS weapon_shotgun, "WEAPON_GRENADE" AS weapon_grenade, "POWERUP_NINJA" AS powerup_ninja, "WEAPON_RIFLE" AS weapon_rifle, "LASER_STOP" AS laser_stop, "CRAZY_SHOTGUN" AS crazy_shotgun, "DRAGGER" AS dragger, "DOOR" AS door, "SWITCH_TIMED" AS switch_timed, "SWITCH" AS switch, "STOP" AS stop, "THROUGH_ALL" AS through_all, "TUNE" AS tune, "OLDLASER" AS oldlaser, "TELEINEVIL" AS teleinevil, "TELEIN" AS telein, "TELECHECK" AS telecheck, "TELEINWEAPON" AS teleinweapon, "TELEINHOOK" AS teleinhook, "CHECKPOINT_FIRST" AS checkpoint_first, "BONUS" AS bonus, "BOOST" AS boost, "PLASMAF" AS plasmaf, "PLASMAE" AS plasmae, "PLASMAU" AS plasmau FROM record_mapinfo;

-- TODO: Why is table reference l ambiguous?
-- materialize=> CREATE OR REPLACE VIEW ranks
--   AS SELECT l.map, l.minTime, race.timestamp, l.count, l.minTimestamp, SUBSTRING(race.server, 1, 3) FROM (
--     SELECT "map", name, min(time) as minTime, name, count(*), min(timestamp) as minTimestamp,
--       ROW_NUMBER() OVER (PARTITION BY "map" ORDER BY min(time) ASC) as row_num
--     FROM race
--     GROUP BY "map", name
--   ) l
--   JOIN race
--   ON race.map = l.map AND race.time = l.minTime and race.name = l.name
--   WHERE row_num <= 20;
-- ERROR:  table reference "l" is ambiguous
CREATE OR REPLACE MATERIALIZED VIEW ranks2
IN CLUSTER compute_cluster
AS SELECT grp.map,
       l.player AS name,
       l.minTime,
       race.timestamp,
       l.count,
       l.minTimestamp,
       SUBSTRING(race.server, 1, 3) AS server
FROM (SELECT DISTINCT "map" FROM race) grp
CROSS JOIN LATERAL (
    SELECT name AS player,
           MIN(time) AS minTime,
           COUNT(*) AS count,
           MIN(timestamp) AS minTimestamp
    FROM race r
    WHERE r.map = grp.map
    GROUP BY name
    OPTIONS (LIMIT INPUT GROUP SIZE 65535, AGGREGATE INPUT GROUP SIZE 255)
    ORDER BY MIN(time) ASC
    LIMIT 20
) l
JOIN race
  ON race.map = grp.map
 AND race.name = l.player
 AND race.time = l.minTime
ORDER BY grp.map, l.minTime;
CREATE INDEX ranks_map IN CLUSTER serving_cluster ON ranks ("map");
-- Use with: select * from ranks where "map" = 'Multeasymap' order by minTime;

CREATE OR REPLACE MATERIALIZED VIEW most_finishes
IN CLUSTER compute_cluster
AS SELECT grp.map,
       l.name,
       l.count,
       l.sum,
       l.min,
       l.max
FROM (SELECT DISTINCT "map" FROM race) grp
CROSS JOIN LATERAL (
    SELECT name,
           COUNT(*) AS count,
           SUM(time) AS sum,
           MIN(timestamp) AS min,
           MAX(timestamp) AS max
    FROM race r
    WHERE r.map = grp.map
    GROUP BY name
    ORDER BY COUNT(*) DESC
    LIMIT 20
) l
ORDER BY grp.map, l.count DESC;
CREATE INDEX most_finishes_map IN CLUSTER serving_cluster ON most_finishes ("map", count);

-- MariaDB: select distinct r.Name, r.ID, r.Time, r.Timestamp, (select substring(Server, 1, 3) from record_race where Map = r.Map and Name = r.Name and Time = r.Time limit 1) as Server from ((select distinct ID from record_teamrace where Map = '%s' ORDER BY Time limit 20) as l) left join record_teamrace as r on l.ID = r.ID order by r.Time, r.ID, r.Name;
CREATE OR REPLACE MATERIALIZED VIEW team_ranks
IN CLUSTER compute_cluster
  AS SELECT teamrace."map",
       teamrace.name,
       teamrace.id,
       teamrace.time,
       teamrace.timestamp,
       (SELECT server
          FROM race
         WHERE race."map" = teamrace."map"
           AND race.name = teamrace.name
           AND race.time = teamrace.time
         LIMIT 1) AS server
FROM (SELECT DISTINCT "map" FROM teamrace) grp
CROSS JOIN LATERAL (
    SELECT id,
           MIN(time) AS minTime
    FROM teamrace t
    WHERE t."map" = grp."map"
    GROUP BY id
    ORDER BY MIN(time) ASC
    LIMIT 20
) l
JOIN teamrace
  ON teamrace.id = l.id
 AND teamrace."map" = grp."map"
 AND teamrace.time = l.minTime;
CREATE INDEX team_ranks_map IN CLUSTER serving_cluster ON team_ranks ("map");
-- Use with select * from team_ranks where "map" = 'Multeasymap' order by time;

-- MariaDB: select (select median(Time) over (partition by Map) from record_race where Map = '%s' %s limit 1), min(Timestamp), max(Timestamp), count(*), count(distinct Name) from record_race where Map = '%s' %s
-- Doesn't support median yet: percentile_cont WITHIN GROUP in postgres, probably won't be, requires recalculation see https://materialize.com/blog/postgres-compatibility/
CREATE OR REPLACE MATERIALIZED VIEW stats
IN CLUSTER compute_cluster
  AS SELECT "map", avg(time), min(timestamp), max(timestamp), count(*), count(distinct Name) as count_distinct
    FROM race
    GROUP BY "map";
CREATE INDEX stats_map IN CLUSTER serving_cluster ON stats ("map");
-- Use: select * from stats where "map" = 'Multeasymap';

-- MariaDB: select count(Name) from record_teamrace where Map = '%s' group by ID order by count(Name) desc limit 1;
CREATE OR REPLACE MATERIALIZED VIEW largest_team
IN CLUSTER compute_cluster
  AS (
  SELECT grp."map",
       l.count
FROM (SELECT DISTINCT "map" FROM teamrace) grp
CROSS JOIN LATERAL (
    SELECT COUNT(name) AS count
    FROM teamrace t
    WHERE t."map" = grp."map"
    GROUP BY id
    ORDER BY COUNT(name) DESC
    LIMIT 1
) l);
CREATE INDEX largest_team_map IN CLUSTER serving_cluster ON largest_team ("map");
-- Use: select * from largest_team where "map" = 'Multeasymap';

-- Now for country-specific queries:
CREATE OR REPLACE MATERIALIZED VIEW ranks_server
IN CLUSTER compute_cluster
AS SELECT grp."map",
       l.player AS name,
       l.minTime,
       race.timestamp,
       l.count,
       l.minTimestamp,
       l.server
FROM (SELECT DISTINCT server, "map" FROM race) grp
CROSS JOIN LATERAL (
    SELECT name AS player,
           MIN(time)       AS minTime,
           COUNT(*)        AS count,
           MIN(timestamp)  AS minTimestamp,
           server
    FROM race r
    WHERE r.server = grp.server
      AND r."map" = grp."map"
    GROUP BY name, server
    ORDER BY MIN(time) ASC
    LIMIT 20
) l
JOIN race
  ON race.server = grp.server
 AND race."map"   = grp."map"
 AND race.name    = l.player
 AND race.time    = l.minTime
ORDER BY grp.server, grp."map", l.minTime;
CREATE INDEX ranks_server_map IN CLUSTER serving_cluster ON ranks_server ("map", server);
-- Use with: select * from ranks_server where "map" = 'Multeasymap' and server = 'GER' order by minTime;

CREATE OR REPLACE MATERIALIZED VIEW team_ranks_server
IN CLUSTER compute_cluster
  AS SELECT teamrace.map, teamrace.name, teamrace.id, time, timestamp, server
  FROM teamrace
  JOIN
  (SELECT DISTINCT id, server, teamrace.map, ROW_NUMBER() OVER (PARTITION BY race.server, teamrace.map ORDER BY min(teamrace.time)) as row_num
    FROM teamrace
    JOIN race ON teamrace.map = race.map and teamrace.name = race.name and teamrace.time = race.time
    GROUP BY race.server, teamrace.map, id) l
  ON l.id = teamrace.id and l.map = teamrace.map AND l.row_num <= 20;
CREATE INDEX team_ranks_server_map IN CLUSTER serving_cluster ON team_ranks_server ("map", server);
-- Use with select * from team_ranks_server where server = 'GER' and "map" = 'Multeasymap' order by time;

CREATE OR REPLACE MATERIALIZED VIEW largest_team_server
IN CLUSTER compute_cluster
  AS (SELECT server, "map", count FROM (
        SELECT server, teamrace.map, count(teamrace.name),
          ROW_NUMBER() OVER (PARTITION BY server ORDER BY count(teamrace.name) DESC) AS row_num
        FROM teamrace
        JOIN race ON teamrace.map = race.map and teamrace.name = race.name and teamrace.time = race.time
        GROUP BY server, teamrace.map, id
        ORDER BY count(teamrace.name))
      WHERE row_num = 1);
CREATE INDEX largest_team_map_server IN CLUSTER serving_cluster ON largest_team_server ("map", server);
-- Use: select * from largest_team_server where server = 'GER' and "map" = 'Multeasymap';

CREATE OR REPLACE MATERIALIZED VIEW most_finishes_server
IN CLUSTER compute_cluster
  AS SELECT server, "map", name, count, sum, min, max FROM (
    SELECT server, "map", name, count(*), sum(time), min(timestamp), max(timestamp),
      ROW_NUMBER() OVER (PARTITION BY "map" ORDER BY count(*) DESC) AS row_num
    FROM race
    GROUP BY server, "map", name
  ) WHERE row_num <= 20;
CREATE INDEX most_finishes_server_map IN CLUSTER serving_cluster ON most_finishes_server ("map", server);
-- Use: select * from most_finishes_server where server = 'GER' and "map" = 'Multeasymap';

CREATE OR REPLACE MATERIALIZED VIEW stats_server
IN CLUSTER compute_cluster
  AS SELECT server, "map", avg(time), min(timestamp), max(timestamp), count(*), count(distinct Name) as count_distinct
    FROM race
    GROUP BY server, "map";
CREATE INDEX stats_server_map IN CLUSTER serving_cluster ON stats_server ("map", server);
-- Use with select * from stats_server where server = 'GER' and "map" = 'Multeasymap';
