SELECT 'Upgrading Sentry store schema from 1.4.0-cdh5 to 1.4.0-cdh5-2';
\i 004-SENTRY-BUGFIX.postgres.sql;

UPDATE "SENTRY_VERSION" SET "SCHEMA_VERSION"='1.4.0-cdh5-2', "VERSION_COMMENT"='Sentry release version 1.4.0-cdh5-2' WHERE "VER_ID"=1;
SELECT 'Finished upgrading Sentry store schema from 1.4.0-cdh5 to 1.4.0-cdh5-2';
