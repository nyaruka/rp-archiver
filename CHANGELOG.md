v8.2.0 (2023-07-31)
-------------------------
 * Update .gitignore

v8.1.7 (2023-06-06)
-------------------------
 * Fix release CHANGELOG generation

v8.1.6 (2023-06-06)
-------------------------
 * Remove deleting of channel logs as these are no longer linked to messages

v8.1.5 (2023-03-24)
-------------------------
 * Revert to go 1.19

v8.1.4 (2023-03-15)
-------------------------
 * Match API and always return type=text|voice for messages

v8.1.3 (2023-03-09)
-------------------------
 * Update dependencies and use go 1.20
 * Update test database schema and cleanup sql queries

v8.1.2 (2023-02-20)
-------------------------
 * Add support for msg_type = T

v8.1.1 (2023-02-15)
-------------------------
 * Don't try to delete broadcast URNs which no longer exist

v8.1.0 (2023-01-18)
-------------------------
 * Delete old flow starts after deleting runs

v8.0.0 (2023-01-09)
-------------------------
 * Only fetch broadcasts which don't have messages
 * Remove use of deprecated ioutil package
 * Update testdb.sql to reflect schema changes and cleanup sql variables
 * Test against postgres 14

v7.5.0
----------
 * Use go 1.19
 * Allow AWS Cred Chain

v7.4.0
----------
 * Include rollups in monthlies failed metric as well as monthlies created from scratch

v7.3.7
----------
 * Change query used to update rollup_id on dailies
 * Remove temporary logging

v7.3.6
----------
 * Add temporary additional logging
 * Replace ExitOnCompletion config option with Once which makes it run once and exit

v7.3.5
----------
 * Improve librato analytics and add tests

v7.3.4
----------
 * Rework stats reporting
 * Log version at startup

v7.3.3
----------
 * Fix parsing start times after midday

v7.3.2
----------
 * Don't log entire run JSON on error, just UUID
 * Make archival happen at configured start time even on first pass

v7.3.1
----------
 * Add librato analytics for time elapsed and number of orgs, msgs and runs

v7.3.0
----------
 * Update to go 1.18 and upgrade dependencies
 * Add support for Msg.visibility=X (deleted by sender)
 * Add arm64 as a build target

v7.2.0
----------
 * Tweak README

v7.1.6
----------
 * Stop setting delete_reason on runs before deletion

v7.1.5
----------
 * Stop updating msgs_msg.delete_reason which is no longer needed

v7.1.4
----------
 * Record flow on msgs

v7.1.3
----------
 * Remove deletion of recent runs as these are no longer created

v7.1.2
----------
 * Use run status instead of is_active and exit_type
 * No longer include events in run archives

v7.1.1
----------
 * Remove references to flowrun.parent_id which is no longer set by mailroom

v7.1.0
----------
 * Remove msgs_msg.response_to_id

v7.0.0
----------
 * Test on PG12 and 13

v6.5.0
----------
 * Limit paths in archived runs to first 500 steps
 * Use go 1.17

v6.4.0
----------
 * 6.4.0 Release Candidate

v6.3.0
----------
 * Don't try to load org languages

v6.2.0
----------
 * Bump CI testing to PG 11 and 12
 * 6.2.0 RC

v6.0.3
----------
 * log next day even when not sleeping

v6.0.2
----------
 * Fix next archive building calculation

v6.0.1
----------
 * Clean up archive file if there is a problem while uploading to s3 or writing to DB
 * Fix NPE when out of disk space

v6.0.0
----------
 * Update README

v5.7.0
----------
 * Add switch to specify start time of archival builds (thanks resistbot)
 * Add switch to exit on build completion (thanks resistbot)

v5.6.0
----------
 * 5.6.0 Release

v5.4.0 
----------
 * 5.4 Release

v5.2.0
----------
 * Sync release with RapidPro 5.2
 * Add PostgreSQL 11 tests

v2.0.1
----------
 * update table references according to v5.2 schema, use wrapf for errors

v2.0.0
----------
 * remove reading is_test on contact

v1.0.8
----------
 * up max connections to two since we need cursor when deleting broadcasts

v1.0.7
----------
 * delete broadcasts which no longer have any active messages

v1.0.6
----------
 * fix travis deploy

v1.0.5
----------
 * IMPORTANT: you must make sure that all your purged broadcasts have been archived before
   removing the recipients table (in RapidPro release)
 * remove archival of purged broadcasts in preparation of removal of recipients table

v1.0.4
----------
 * IMPORTANT: you must make sure that all your purged broadcasts have been archived before
   removing the recipients table (in RapidPro release)
 * remove archival of purged broadcasts in preparation of removal of recipients table

v1.0.3
----------
 * convert to go module
 * add testing for pg 10
 * properly archive surveyor messages

v1.0.2
----------
 * give ourselves up to 3 hours per archive deletion, 15 mins per transaction

v1.0.1
----------
 * add uuid to run archives

v1.0.0
----------
* 1.0 release
* be more specific in our reference to modified_on

v0.0.27
----------
 * add modified-on to message archives

v0.0.26
----------
 * make sure sent status is written for messages
 * fix run.values export format

v0.0.25
----------
 * add submitted_by to flow runs, populate with username of user that submitted

v0.0.24
----------
 * revert change to message type, purged messages from broadcasts should be flow

v0.0.22
----------
 * use primary_language field for default language for org

v0.0.21
----------
 * create purged messages from broadcasts

v0.0.20
----------
 * update docs, more consistent command line
 * turn archiver into service
 * deletion_date->deleted_on

v0.0.19
----------
 * increase timeout for calculation of all run ids in an archive

v0.0.18
----------
 * add deletion of runs

v0.0.17
----------
 * add support for multipart uploads and archives > 5 gigs

v0.0.16
----------
 * add deletion_date field, write upon deletion

v0.0.15
----------
 * better context management in archival deletion

v0.0.14
----------
 * bump batch size down to 100

v0.0.13
----------
 * dont try to build archives that are too big

v0.0.12
----------
 * allow msgs with null channels, dont archive test contacts

v0.0.11
----------
 * deletion of messages after archiving
 * more logging, add status logging of deletions
 * more tests, remove cascades so we test accurately, rollups dont need purging
 * correct set of incantations to get UTC dates out of golang/pg

v0.0.10
----------
 * increase unit test coverage of rollup cases

v0.0.9
----------
 * create montly archives when doing backfills, add input and value to run outputs

v0.0.8
----------
 * don't download 0 record archives when building monthlies

v0.0.7
----------
 * archive flow runs based on modified instead of created

v0.0.6
----------
 * add tests for flow runs, test file contents as well
 * add writing of rollup id to writing of monthlies to db
 * add montly rollups based on day archives
 * don't serialize urns for anon orgs, fix attachment serialization
 * use JSONL - line delimted JSON as archive format

v0.0.5
----------
 * add request tracing at debug level

v0.0.4
----------
 * switch to md5 hashes, verify upon upload, better logs

v0.0.3
----------
 * tweak config var for bucket name
 * ignore deleted messages when archiving

