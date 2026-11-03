-- Down migration for 135_koi_write_log.sql
DROP TABLE IF EXISTS koi_write_log;
DELETE FROM koi_migrations WHERE migration_id = '135_koi_write_log';
