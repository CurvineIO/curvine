# SQLite transfer store upgrade and rollback

Opening a transfer database with the job-specification and task-result support upgrades its SQLite schema to version 3. Older binaries reject version 3 even if the database contains only Load and Export jobs. Ignoring the added columns is not a supported downgrade.

Before upgrading, stop transfer writers and take a consistent SQLite backup (for example, with SQLite's backup command). To roll back the binary, stop the new writers and restore the pre-upgrade backup before starting the old binary. Changes made after the backup will not be present in the restored database. Do not lower the schema version by hand.
