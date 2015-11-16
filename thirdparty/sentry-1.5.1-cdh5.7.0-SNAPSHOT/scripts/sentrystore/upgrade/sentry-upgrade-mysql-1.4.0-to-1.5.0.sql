SELECT 'Upgrading Sentry store schema from 1.4.0 to 1.5.0' AS ' ';
SOURCE 001-SENTRY-327.mysql.sql;
SOURCE 002-SENTRY-339.mysql.sql;
SOURCE 003-SENTRY-380.mysql.sql;
SOURCE 004-SENTRY-74.mysql.sql;
SOURCE 005-SENTRY-398.mysql.sql;

UPDATE SENTRY_VERSION SET SCHEMA_VERSION='1.5.0', VERSION_COMMENT='Sentry release version 1.5.0' WHERE VER_ID=1;
SELECT 'Finish upgrading Sentry store schema from 1.4.0 to 1.5.0' AS ' ';

