-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT extversion < '2.0.0' AS has_refresh_mat_view from pg_extension WHERE extname = 'timescaledb' \gset

-- disable background workers to prevent deadlocks between background processes
-- on timescaledb 1.7.x
SELECT _timescaledb_internal.stop_background_workers();

CREATE TYPE custom_type AS (high int, low int);

CREATE TABLE conditions_before (
      timec       TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null,
      highlow     custom_type null,
      bit_int     smallint,
      good_life   boolean
    );

SELECT table_name FROM create_hypertable( 'conditions_before', 'timec');

INSERT INTO conditions_before
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 55, 75, 40, 70, NULL, (1,2)::custom_type, 2, true;
INSERT INTO conditions_before
SELECT generate_series('2018-11-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'NYC', 35, 45, 50, 40, NULL, (3,4)::custom_type, 4, false;
INSERT INTO conditions_before
SELECT generate_series('2018-11-01 00:00'::timestamp, '2018-12-15 00:00'::timestamp, '1 day'), 'LA', 73, 55, NULL, 28, NULL, NULL, 8, true;

DO LANGUAGE PLPGSQL $$
DECLARE
  ts_version TEXT;
BEGIN
  SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';

  IF ts_version < '2.0.0' THEN
    CREATE VIEW mat_before
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag='-30 day', timescaledb.max_interval_per_job ='1000 day')
    AS
      SELECT time_bucket('1week', timec) as bucket,
	location,
	min(allnull) as min_allnull,
	max(temperature) as max_temp,
	sum(temperature)+sum(humidity) as agg_sum_expr,
	avg(humidity),
	stddev(humidity),
	bit_and(bit_int),
	bit_or(bit_int),
	bool_and(good_life),
	every(temperature > 0),
	bool_or(good_life),
	count(*) as count_rows,
	count(temperature) as count_temp,
	count(allnull) as count_zero,
	corr(temperature, humidity),
	covar_pop(temperature, humidity),
	covar_samp(temperature, humidity),
	regr_avgx(temperature, humidity),
	regr_avgy(temperature, humidity),
	regr_count(temperature, humidity),
	regr_intercept(temperature, humidity),
	regr_r2(temperature, humidity),
	regr_slope(temperature, humidity),
	regr_sxx(temperature, humidity),
	regr_sxy(temperature, humidity),
	regr_syy(temperature, humidity),
	stddev(temperature) as stddev_temp,
	stddev_pop(temperature),
	stddev_samp(temperature),
	variance(temperature),
	var_pop(temperature),
	var_samp(temperature),
	last(temperature, timec) as last_temp,
	last(highlow, timec) as last_hl,
	first(highlow, timec) as first_hl,
	histogram(temperature, 0, 100, 5)
      FROM conditions_before
      GROUP BY bucket, location
      HAVING min(location) >= 'NYC' and avg(temperature) > 2;

  ELSE
    CREATE MATERIALIZED VIEW IF NOT EXISTS mat_before
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true)
    AS
      SELECT time_bucket('1week', timec) as bucket,
	location,
	min(allnull) as min_allnull,
	max(temperature) as max_temp,
	sum(temperature)+sum(humidity) as agg_sum_expr,
	avg(humidity),
	stddev(humidity),
	bit_and(bit_int),
	bit_or(bit_int),
	bool_and(good_life),
	every(temperature > 0),
	bool_or(good_life),
	count(*) as count_rows,
	count(temperature) as count_temp,
	count(allnull) as count_zero,
	corr(temperature, humidity),
	covar_pop(temperature, humidity),
	covar_samp(temperature, humidity),
	regr_avgx(temperature, humidity),
	regr_avgy(temperature, humidity),
	regr_count(temperature, humidity),
	regr_intercept(temperature, humidity),
	regr_r2(temperature, humidity),
	regr_slope(temperature, humidity),
	regr_sxx(temperature, humidity),
	regr_sxy(temperature, humidity),
	regr_syy(temperature, humidity),
	stddev(temperature) as stddev_temp,
	stddev_pop(temperature),
	stddev_samp(temperature),
	variance(temperature),
	var_pop(temperature),
	var_samp(temperature),
	last(temperature, timec) as last_temp,
	last(highlow, timec) as last_hl,
	first(highlow, timec) as first_hl,
	histogram(temperature, 0, 100, 5)
      FROM conditions_before
      GROUP BY bucket, location
      HAVING min(location) >= 'NYC' and avg(temperature) > 2 WITH NO DATA;
    PERFORM add_continuous_aggregate_policy('mat_before', NULL, '-30 days'::interval, '336 h');
  END IF;
END $$;

\if :has_refresh_mat_view
REFRESH MATERIALIZED VIEW mat_before;
\else
CALL refresh_continuous_aggregate('mat_before',NULL,NULL);
\endif

-- we create separate schema for realtime agg since we dump all view definitions in public schema
-- but realtime agg view definition is not stable across versions
CREATE SCHEMA cagg;

DO LANGUAGE PLPGSQL $$
DECLARE
  ts_version TEXT;
BEGIN
  SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';

  IF ts_version < '2.0.0' THEN
    CREATE VIEW cagg.realtime_mat
    WITH ( timescaledb.continuous, timescaledb.materialized_only=false, timescaledb.refresh_lag='-30 day', timescaledb.max_interval_per_job ='1000 day')
    AS
      SELECT time_bucket('1week', timec) as bucket,
	location,
	min(allnull) as min_allnull,
	max(temperature) as max_temp,
	sum(temperature)+sum(humidity) as agg_sum_expr,
	avg(humidity),
	stddev(humidity),
	bit_and(bit_int),
	bit_or(bit_int),
	bool_and(good_life),
	every(temperature > 0),
	bool_or(good_life),
	count(*) as count_rows,
	count(temperature) as count_temp,
	count(allnull) as count_zero,
	corr(temperature, humidity),
	covar_pop(temperature, humidity),
	covar_samp(temperature, humidity),
	regr_avgx(temperature, humidity),
	regr_avgy(temperature, humidity),
	regr_count(temperature, humidity),
	regr_intercept(temperature, humidity),
	regr_r2(temperature, humidity),
	regr_slope(temperature, humidity),
	regr_sxx(temperature, humidity),
	regr_sxy(temperature, humidity),
	regr_syy(temperature, humidity),
	stddev(temperature) as stddev_temp,
	stddev_pop(temperature),
	stddev_samp(temperature),
	variance(temperature),
	var_pop(temperature),
	var_samp(temperature),
	last(temperature, timec) as last_temp,
	last(highlow, timec) as last_hl,
	first(highlow, timec) as first_hl,
	histogram(temperature, 0, 100, 5)
      FROM conditions_before
      GROUP BY bucket, location
      HAVING min(location) >= 'NYC' and avg(temperature) > 2;
  ELSE
    CREATE MATERIALIZED VIEW IF NOT EXISTS cagg.realtime_mat
    WITH ( timescaledb.continuous, timescaledb.materialized_only=false)
    AS
      SELECT time_bucket('1week', timec) as bucket,
	location,
	min(allnull) as min_allnull,
	max(temperature) as max_temp,
	sum(temperature)+sum(humidity) as agg_sum_expr,
	avg(humidity),
	stddev(humidity),
	bit_and(bit_int),
	bit_or(bit_int),
	bool_and(good_life),
	every(temperature > 0),
	bool_or(good_life),
	count(*) as count_rows,
	count(temperature) as count_temp,
	count(allnull) as count_zero,
	corr(temperature, humidity),
	covar_pop(temperature, humidity),
	covar_samp(temperature, humidity),
	regr_avgx(temperature, humidity),
	regr_avgy(temperature, humidity),
	regr_count(temperature, humidity),
	regr_intercept(temperature, humidity),
	regr_r2(temperature, humidity),
	regr_slope(temperature, humidity),
	regr_sxx(temperature, humidity),
	regr_sxy(temperature, humidity),
	regr_syy(temperature, humidity),
	stddev(temperature) as stddev_temp,
	stddev_pop(temperature),
	stddev_samp(temperature),
	variance(temperature),
	var_pop(temperature),
	var_samp(temperature),
	last(temperature, timec) as last_temp,
	last(highlow, timec) as last_hl,
	first(highlow, timec) as first_hl,
	histogram(temperature, 0, 100, 5)
      FROM conditions_before
      GROUP BY bucket, location
      HAVING min(location) >= 'NYC' and avg(temperature) > 2 WITH NO DATA;
    PERFORM add_continuous_aggregate_policy('cagg.realtime_mat', NULL, '-30 days'::interval, '336 h');
  END IF;
END $$;

\if :has_refresh_mat_view
REFRESH MATERIALIZED VIEW cagg.realtime_mat;
\else
CALL refresh_continuous_aggregate('cagg.realtime_mat',NULL,NULL);
\endif

-- test ignore_invalidation_older_than migration --
DO LANGUAGE PLPGSQL $$
DECLARE
  ts_version TEXT;
BEGIN
  SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';

  IF ts_version < '2.0.0' THEN
    CREATE VIEW mat_ignoreinval
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true,
           timescaledb.refresh_lag='-30 day',
           timescaledb.ignore_invalidation_older_than='30 days',
           timescaledb.max_interval_per_job = '100000 days')
    AS
      SELECT time_bucket('1 week', timec) as bucket,
    max(temperature) as maxtemp
      FROM conditions_before
      GROUP BY bucket;
  ELSE
    CREATE MATERIALIZED VIEW IF NOT EXISTS  mat_ignoreinval
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true)
    AS
      SELECT time_bucket('1 week', timec) as bucket,
    max(temperature) as maxtemp
      FROM conditions_before
      GROUP BY bucket WITH NO DATA;
    
    PERFORM add_continuous_aggregate_policy('mat_ignoreinval', '30 days'::interval, '-30 days'::interval, '336 h');
  END IF;
END $$;

\if :has_refresh_mat_view
REFRESH MATERIALIZED VIEW mat_ignoreinval;
\else
CALL refresh_continuous_aggregate('mat_ignoreinval',NULL,NULL);
\endif

-- test new data beyond the invalidation threshold is properly handled --
CREATE TABLE inval_test (time TIMESTAMPTZ, location TEXT, temperature DOUBLE PRECISION);
SELECT create_hypertable('inval_test', 'time', chunk_time_interval => INTERVAL '1 week');
 
INSERT INTO inval_test
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-20 00:00'::timestamp, '1 day'), 'POR', generate_series(40.5, 50.0, 0.5);
INSERT INTO inval_test
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-20 00:00'::timestamp, '1 day'), 'NYC', generate_series(31.0, 50.0, 1.0);

DO LANGUAGE PLPGSQL $$
DECLARE
  ts_version TEXT;
BEGIN
  SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';

  IF ts_version < '2.0.0' THEN
    CREATE VIEW mat_inval
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true,
           timescaledb.refresh_lag='-20 days',
           timescaledb.refresh_interval='12 hours',
           timescaledb.max_interval_per_job='100000 days' )
    AS 
      SELECT time_bucket('10 minute', time) as bucket, location, min(temperature) as min_temp,
        max(temperature) as max_temp, avg(temperature) as avg_temp
      FROM inval_test
      GROUP BY bucket, location;

  ELSE
    CREATE MATERIALIZED VIEW mat_inval
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true )
    AS 
      SELECT time_bucket('10 minute', time) as bucket, location, min(temperature) as min_temp, 
        max(temperature) as max_temp, avg(temperature) as avg_temp
      FROM inval_test
      GROUP BY bucket, location WITH NO DATA;

    PERFORM add_continuous_aggregate_policy('mat_inval', NULL, '-20 days'::interval, '12 hours');
  END IF;
END $$;

\if :has_refresh_mat_view
REFRESH MATERIALIZED VIEW mat_inval;
\else
CALL refresh_continuous_aggregate('mat_inval',NULL,NULL);
\endif

INSERT INTO inval_test
SELECT generate_series('2118-12-01 00:00'::timestamp, '2118-12-20 00:00'::timestamp, '1 day'), 'POR', generate_series(135.25, 140.0, 0.25);
INSERT INTO inval_test
SELECT generate_series('2118-12-01 00:00'::timestamp, '2118-12-20 00:00'::timestamp, '1 day'), 'NYC', generate_series(131.0, 150.0, 1.0);

-- Add an integer base table to ensure we handle it correctly
CREATE TABLE int_time_test(timeval integer, col1 integer, col2 integer);
select create_hypertable('int_time_test', 'timeval', chunk_time_interval=> 2);

CREATE OR REPLACE FUNCTION integer_now_test() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(timeval), 0) FROM int_time_test $$;
SELECT set_integer_now_func('int_time_test', 'integer_now_test');

INSERT INTO int_time_test VALUES
(10, - 4, 1), (11, - 3, 5), (12, - 3, 7), (13, - 3, 9), (14,-4, 11),
(15, -4, 22), (16, -4, 23);

DO LANGUAGE PLPGSQL $$
DECLARE
  ts_version TEXT;
BEGIN
  SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';

  IF ts_version < '2.0.0' THEN
    CREATE VIEW mat_inttime
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true,
           timescaledb.ignore_invalidation_older_than = 5,
           timescaledb.refresh_lag = 2,
           timescaledb.refresh_interval='12 hours')
    AS 
      SELECT time_bucket( 2, timeval), COUNT(col1)
      FROM int_time_test
      GROUP BY 1;

    CREATE VIEW mat_inttime2
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true,
           timescaledb.refresh_lag = 2,
           timescaledb.refresh_interval='12 hours')
    AS 
      SELECT time_bucket( 2, timeval), COUNT(col1)
      FROM int_time_test
      GROUP BY 1;

  ELSE
    CREATE MATERIALIZED VIEW mat_inttime
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true )
    AS 
      SELECT time_bucket( 2, timeval), COUNT(col1)
      FROM int_time_test
      GROUP BY 1 WITH NO DATA;

    CREATE MATERIALIZED VIEW mat_inttime2
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true )
    AS 
      SELECT time_bucket( 2, timeval), COUNT(col1)
      FROM int_time_test
      GROUP BY 1 WITH NO DATA;

    PERFORM add_continuous_aggregate_policy('mat_inttime', 5, 2, '12 hours');
    PERFORM add_continuous_aggregate_policy('mat_inttime2', NULL, 2, '12 hours');
  END IF;
END $$;


\if :has_refresh_mat_view
REFRESH MATERIALIZED VIEW mat_inttime;
REFRESH MATERIALIZED VIEW mat_inttime2;
\else
CALL refresh_continuous_aggregate('mat_inttime',NULL,NULL);
CALL refresh_continuous_aggregate('mat_inttime2',NULL,NULL);
\endif
