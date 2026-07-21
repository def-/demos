DROP CLUSTER IF EXISTS quickstart CASCADE;
ALTER ROLE materialize SET cluster TO serving_cluster;
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
CREATE INDEX race_idx IN CLUSTER compute_cluster ON race ("map", name, time);
CREATE OR REPLACE VIEW teamrace AS SELECT "Map" AS map, "Name" as name, cast("Timestamp" as timestamp) as timestamp, "Time" as time, "ID" as id, "GameID" as gameid FROM record_teamrace;
CREATE INDEX teamrace_idx IN CLUSTER compute_cluster ON teamrace (id, "map", time);
CREATE OR REPLACE VIEW maps AS SELECT "Map" AS map, "Server" as server, "Points" as points, "Stars" as stars, "Mapper" as mapper, cast("Timestamp" as timestamp) as timestamp FROM record_maps;

CREATE OR REPLACE VIEW mappers AS SELECT "Mapper" AS Mapper, "NumMaps" as nummaps FROM record_mappers;
CREATE VIEW mapinfo AS SELECT "Map" AS map, "Width" as width, "Height" as height, "DEATH" as death, "THROUGH" as through, "JUMP" as jump, "DFREEZE" AS dfreeze, "EHOOK_START" AS ehook_start, "HIT_END" AS hit_end, "SOLO_START" AS solo_start, "TELE_GUN" AS tele_gun, "TELE_GRENADE" AS tele_grenade, "TELE_LASER" AS tele_laser, "NPC_START" AS npc_start, "SUPER_START" AS super_start, "JETPACK_START" AS jetpack_start, "WALLJUMP" AS walljump, "NPH_START" AS nph_start, "WEAPON_SHOTGUN" AS weapon_shotgun, "WEAPON_GRENADE" AS weapon_grenade, "POWERUP_NINJA" AS powerup_ninja, "WEAPON_RIFLE" AS weapon_rifle, "LASER_STOP" AS laser_stop, "CRAZY_SHOTGUN" AS crazy_shotgun, "DRAGGER" AS dragger, "DOOR" AS door, "SWITCH_TIMED" AS switch_timed, "SWITCH" AS switch, "STOP" AS stop, "THROUGH_ALL" AS through_all, "TUNE" AS tune, "OLDLASER" AS oldlaser, "TELEINEVIL" AS teleinevil, "TELEIN" AS telein, "TELECHECK" AS telecheck, "TELEINWEAPON" AS teleinweapon, "TELEINHOOK" AS teleinhook, "CHECKPOINT_FIRST" AS checkpoint_first, "BONUS" AS bonus, "BOOST" AS boost, "PLASMAF" AS plasmaf, "PLASMAE" AS plasmae, "PLASMAU" AS plasmau FROM record_mapinfo;

-- Shared per-(map,name) aggregate: ONE MIN/MAX/SUM/COUNT reduction over race,
-- reused by the global ranks / most_finishes leaderboards (avoids each rebuilding
-- its own hierarchical MIN/MAX tower over the full race collection).
CREATE OR REPLACE VIEW agg_map_name AS
SELECT map, name,
       COUNT(*)       AS finishes,
       MIN(time)      AS min_time,
       SUM(time)      AS sum_time,
       MIN(timestamp) AS min_ts,
       MAX(timestamp) AS max_ts
FROM race
GROUP BY map, name
OPTIONS (AGGREGATE INPUT GROUP SIZE 255);
CREATE INDEX agg_map_name_idx IN CLUSTER compute_cluster ON agg_map_name (map);

-- Shared per-(map,server,name) aggregate: reused by the per-server leaderboards.
CREATE OR REPLACE VIEW agg_map_server_name AS
SELECT map, server, name,
       COUNT(*)       AS finishes,
       MIN(time)      AS min_time,
       SUM(time)      AS sum_time,
       MIN(timestamp) AS min_ts,
       MAX(timestamp) AS max_ts
FROM race
GROUP BY map, server, name
OPTIONS (AGGREGATE INPUT GROUP SIZE 255);
CREATE INDEX agg_map_server_name_idx IN CLUSTER compute_cluster ON agg_map_server_name (map, server);

CREATE OR REPLACE MATERIALIZED VIEW ranks
IN CLUSTER compute_cluster
AS SELECT grp.map,
       l.name,
       l.min_time AS minTime,
       race.timestamp,
       l.finishes AS count,
       l.min_ts AS minTimestamp,
       SUBSTRING(race.server, 1, 3) AS server
FROM (SELECT DISTINCT map FROM agg_map_name) grp
CROSS JOIN LATERAL (
    SELECT name, min_time, finishes, min_ts
    FROM agg_map_name a
    WHERE a.map = grp.map
    OPTIONS (LIMIT INPUT GROUP SIZE 65535)
    ORDER BY min_time ASC
    LIMIT 20
) l
JOIN race
  ON race.map = grp.map
 AND race.name = l.name
 AND race.time = l.min_time
ORDER BY grp.map, l.min_time;
CREATE INDEX ranks_map IN CLUSTER serving_cluster ON ranks ("map");
-- Use with: select * from ranks where "map" = 'Multeasymap' order by minTime;

CREATE OR REPLACE MATERIALIZED VIEW most_finishes
IN CLUSTER compute_cluster
AS SELECT grp.map,
       l.name,
       l.finishes AS count,
       l.sum_time AS sum,
       l.min_ts AS min,
       l.max_ts AS max
FROM (SELECT DISTINCT map FROM agg_map_name) grp
CROSS JOIN LATERAL (
    SELECT name, finishes, sum_time, min_ts, max_ts
    FROM agg_map_name a
    WHERE a.map = grp.map
    OPTIONS (LIMIT INPUT GROUP SIZE 65535)
    ORDER BY finishes DESC
    LIMIT 20
) l
ORDER BY grp.map, l.finishes DESC;
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
         OPTIONS (LIMIT INPUT GROUP SIZE 15)
         LIMIT 1) AS server
FROM (SELECT DISTINCT "map" FROM teamrace) grp
CROSS JOIN LATERAL (
    SELECT id,
           MIN(time) AS minTime
    FROM teamrace t
    WHERE t."map" = grp."map"
    GROUP BY id
    OPTIONS (LIMIT INPUT GROUP SIZE 4095, AGGREGATE INPUT GROUP SIZE 15)
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
  AS SELECT map,
       SUM(sum_time)::float8 / SUM(finishes) AS avg,
       MIN(min_ts) AS min,
       MAX(max_ts) AS max,
       SUM(finishes) AS count,
       COUNT(*) AS count_distinct
    FROM agg_map_name
    GROUP BY map
    OPTIONS (AGGREGATE INPUT GROUP SIZE 16777215);
CREATE INDEX stats_map IN CLUSTER serving_cluster ON stats ("map");
-- Use: select * from stats where "map" = 'Multeasymap';

CREATE OR REPLACE MATERIALIZED VIEW team_stats
IN CLUSTER compute_cluster
  AS SELECT "map", count(distinct id)
    FROM teamrace
    GROUP BY "map";
CREATE INDEX team_stats_map IN CLUSTER serving_cluster ON team_stats ("map");

CREATE OR REPLACE MATERIALIZED VIEW team_stats_server
IN CLUSTER compute_cluster
  AS SELECT teamrace."map", server, count(distinct id)
    FROM teamrace
    LEFT JOIN race
    ON teamrace."map" = race."map" and teamrace.name = race.name and teamrace.time = race.time
    GROUP BY teamrace."map", server;
CREATE INDEX team_stats_server_map_server IN CLUSTER serving_cluster ON team_stats_server ("map", server);

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
    OPTIONS (LIMIT INPUT GROUP SIZE 15)
    ORDER BY COUNT(name) DESC
    LIMIT 1
) l);
CREATE INDEX largest_team_map IN CLUSTER serving_cluster ON largest_team ("map");
-- Use: select * from largest_team where "map" = 'Multeasymap';

-- Now for country-specific queries:
CREATE OR REPLACE MATERIALIZED VIEW ranks_server
IN CLUSTER compute_cluster
AS SELECT grp.map,
       l.name,
       l.min_time AS minTime,
       race.timestamp,
       l.finishes AS count,
       l.min_ts AS minTimestamp,
       grp.server
FROM (SELECT DISTINCT map, server FROM agg_map_server_name) grp
CROSS JOIN LATERAL (
    SELECT name, min_time, finishes, min_ts
    FROM agg_map_server_name a
    WHERE a.map = grp.map AND a.server = grp.server
    OPTIONS (LIMIT INPUT GROUP SIZE 4095)
    ORDER BY min_time ASC
    LIMIT 20
) l
JOIN race
  ON race.server = grp.server
 AND race.map     = grp.map
 AND race.name    = l.name
 AND race.time    = l.min_time
ORDER BY grp.server, grp.map, l.min_time;
CREATE INDEX ranks_server_map IN CLUSTER serving_cluster ON ranks_server ("map", server);
-- Use with: select * from ranks_server where "map" = 'Multeasymap' and server = 'GER' order by minTime;

CREATE OR REPLACE MATERIALIZED VIEW team_ranks_server
IN CLUSTER compute_cluster
AS
WITH ranked AS (
    SELECT
        teamrace.id,
        teamrace.name,
        teamrace.time,
        teamrace.timestamp,
        teamrace."map",
        race.server,
        DENSE_RANK() OVER (
            PARTITION BY race.server, teamrace."map"
            ORDER BY teamrace.time
        ) AS team_rank
    FROM teamrace
    JOIN race
      ON teamrace."map" = race."map"
     AND teamrace.name = race.name
     AND teamrace.time = race.time
)
SELECT
    ranked."map",
    ranked.name,
    ranked.id,
    ranked.time,
    ranked.timestamp,
    ranked.server
FROM ranked
WHERE ranked.team_rank <= 20
ORDER BY ranked.server, ranked."map", ranked.time;
CREATE INDEX team_ranks_server_map IN CLUSTER serving_cluster ON team_ranks_server ("map", server);
-- Use with select * from team_ranks_server where server = 'GER' and "map" = 'Multeasymap' order by time;

CREATE OR REPLACE MATERIALIZED VIEW largest_team_server
IN CLUSTER compute_cluster
AS
WITH joined AS (
    SELECT
        r.server,
        t."map",
        t.id AS team_id
    FROM teamrace AS t
    JOIN race AS r
      ON t."map" = r."map"
     AND t.name  = r.name
     AND t.time  = r.time
),
counts AS (
    -- size of each team (same id) within (server, map)
    SELECT
        server,
        "map",
        team_id,
        COUNT(*) AS cnt
    FROM joined
    GROUP BY server, "map", team_id
),
max_counts AS (
    -- for each (server, map), what is the largest team size?
    SELECT
        server,
        "map",
        MAX(cnt) AS max_cnt
    FROM counts
    GROUP BY server, "map"
)
SELECT
    server,
    "map",
    max_cnt AS count
FROM max_counts
ORDER BY server, "map";
CREATE INDEX largest_team_map_server IN CLUSTER serving_cluster ON largest_team_server ("map", server);
-- Use: select * from largest_team_server where server = 'GER' and "map" = 'Multeasymap';

CREATE OR REPLACE MATERIALIZED VIEW most_finishes_server
IN CLUSTER compute_cluster AS
SELECT grp.map,
       grp.server,
       l.name,
       l.finishes AS count,
       l.sum_time AS sum,
       l.min_ts AS min,
       l.max_ts AS max
FROM (SELECT DISTINCT map, server FROM agg_map_server_name) grp
CROSS JOIN LATERAL (
    SELECT name, finishes, sum_time, min_ts, max_ts
    FROM agg_map_server_name a
    WHERE a.map = grp.map AND a.server = grp.server
    OPTIONS (LIMIT INPUT GROUP SIZE 4095)
    ORDER BY finishes DESC
    LIMIT 20
) l
ORDER BY grp.map, grp.server, l.finishes DESC;
CREATE INDEX most_finishes_server_map IN CLUSTER serving_cluster ON most_finishes_server ("map", server);
-- Use: select * from most_finishes_server where server = 'GER' and "map" = 'Multeasymap';

CREATE OR REPLACE MATERIALIZED VIEW stats_server
IN CLUSTER compute_cluster
  AS SELECT server, map,
       SUM(sum_time)::float8 / SUM(finishes) AS avg,
       MIN(min_ts) AS min,
       MAX(max_ts) AS max,
       SUM(finishes) AS count,
       COUNT(*) AS count_distinct
    FROM agg_map_server_name
    GROUP BY server, map
    OPTIONS (AGGREGATE INPUT GROUP SIZE 16777215);

CREATE INDEX stats_server_map IN CLUSTER serving_cluster ON stats_server ("map", server);
-- Use with select * from stats_server where server = 'GER' and "map" = 'Multeasymap';

CREATE OR REPLACE MATERIALIZED VIEW last_finishes
IN CLUSTER compute_cluster
AS SELECT
            l.timestamp,
            l.map,
            name,
            time,
            l.server,
            maps.server as type
        FROM
            (
                    (
                                SELECT * FROM race
                                OPTIONS (LIMIT INPUT GROUP SIZE 4095)
                                ORDER BY timestamp DESC
                                LIMIT 20000
                            )
                            AS l
                        JOIN
                            maps
                            ON l.map = maps.map
                )
        ORDER BY timestamp DESC;
CREATE INDEX last_finishes_idx IN CLUSTER serving_cluster ON last_finishes (timestamp);
