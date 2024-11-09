-- S3 secrets Redbad
.read s3-init-private.sql

-- CLI settings
.timer on
.echo on

-- Query processing config
SET THREADS TO 4;
SET enable_progress_bar TO false;

