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

