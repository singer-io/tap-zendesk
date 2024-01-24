# Changelog

## 2.4.0
  * Upgrades to run on python 3.11.7 [#146](https://github.com/singer-io/tap-zendesk/pull/146)

## 2.3.1
  * Dependabot update [#129](https://github.com/singer-io/tap-zendesk/pull/129)
## 2.3.0
  * Adds configurable page size for requests [#141](https://github.com/singer-io/tap-zendesk/pull/141)
## 2.2.0
  * Adds Support for lookup fields [#124](https://github.com/singer-io/tap-zendesk/pull/124)

## 2.1.0
  * Adds new streams `talk_phone_numbers` and `ticket_metric_events` [#111](https://github.com/singer-io/tap-zendesk/pull/111)
## 2.0.1
  * Adds backoff/retry for `ProtocolError` and `ChunkedEncodingError` [#131](https://github.com/singer-io/tap-zendesk/pull/131)
## 2.0.0
  * Incremental Exports API implementation for User's stream [#127](https://github.com/singer-io/tap-zendesk/pull/127)
## 1.7.6
  * Fix Infinite Loop for Users [#103](https://github.com/singer-io/tap-zendesk/pull/103)
## 1.7.5
  * Added support for backoff and retry for error 409 [#107](https://github.com/singer-io/tap-zendesk/pull/107)
  * Code Formatting [#107](https://github.com/singer-io/tap-zendesk/pull/107)
## 1.7.4
  * Request Timeout Implementation [#79](https://github.com/singer-io/tap-zendesk/pull/79)
## 1.7.3
  * 503 error code retry is taken care in ths release [#95](https://github.com/singer-io/tap-zendesk/pull/95)
## 1.7.2
  * 524 and 520 error codes are taken care in this release to retry [93](https://github.com/singer-io/tap-zendesk/pull/93)
## 1.7.1
  * Reverted back API access change login during discover mode [90](https://github.com/singer-io/tap-zendesk/pull/90)
## 1.7.0
  * Removed Buffer System [77](https://github.com/singer-io/tap-zendesk/pull/77)
  * Check API access in discovery mode [74](https://github.com/singer-io/tap-zendesk/pull/74)
  * Comprehensive Error Messaging [69](https://github.com/singer-io/tap-zendesk/pull/69)
  * Other small enhancements including request timeout, following best practicesfor integration testing [78](https://github.com/singer-io/tap-zendesk/pull/78)

## 1.6.0
  * Fixing the via.from field on the tickets, and ticket_comments stream [#75](https://github.com/singer-io/tap-zendesk/pull/75)
    * Remove usage of `zenpy` library for tickets stream and all sub-streams

## 1.5.8
  * Revert Organizations Stream back to the Incremental Search endpoint [#70](https://github.com/singer-io/tap-zendesk/pull/70)

## 1.5.7
  * Limit the Satisfaction Ratings Stream by the bookmarked updated at value [#67](https://github.com/singer-io/tap-zendesk/pull/67)

## 1.5.6
  * Log the id for the users found outside of the queried window [#65](https://github.com/singer-io/tap-zendesk/pull/65)

## 1.5.5
  Changes introduced in [#64](https://github.com/singer-io/tap-zendesk/pull/64)
  * Add to setup.py a "test" extra_requires so that CI doesn't have to install ipdb
  * Add to setup.py dependencies on backoff and requests
  * Add retry logic to the requests made for the Organizations, Satisfaction Ratings, Ticket Fields, Tags, and GroupMemberships streams
  * Add unit tests around our new cursor based pagination logic
  * Update singer-python from 5.2.1 to 5.12.2
  * Update zenpy from 2.0.0 to 2.0.24
  * Update the Organizations, Satisfaction Ratings, Ticket Fields, Tags, GroupMemberships, and Tickets to use cursor-based pagination
    * Tickets leans on zenpy to do pagination
    * The other streams do not
  * Fix Circle Config to install "test" dependencies instead of "dev"

## 1.5.4
  * Log Request URL (and URL params), Response ETag, and Response 'X-Request-Id' header to help with troubleshooting [#63](https://github.com/singer-io/tap-zendesk/pull/63)

## 1.5.3
  * Break out of infinite loop with users stream and bookmark on date window end [#46](https://github.com/singer-io/tap-zendesk/pull/46)

## 1.5.2
  * Add retry logic for users stream [#45](https://github.com/singer-io/tap-zendesk/pull/45)

## 1.5.1
  * Add error message to go along with assert [#44](https://github.com/singer-io/tap-zendesk/pull/44)

## 1.5.0
  * Add date windowing to users stream and satisfaction ratings stream [#42](https://github.com/singer-io/tap-zendesk/pull/42)

## 1.4.7
  * Use `start_time` query parameter for satisfaction_ratings stream [#37](https://github.com/singer-io/tap-zendesk/pull/37)

## 1.4.6
  * Reduce the number of `Transformer` log messages from 1 per record to 1
    per stream [#28](https://github.com/singer-io/tap-zendesk/pull/28)

## 1.4.4
  * Add zendesk partner headers to all requests [#27](https://github.com/singer-io/tap-zendesk/pull/27)

## 1.4.2
  * Add `ticket_id` field to `ticket_comments` stream [#22](https://github.com/singer-io/tap-zendesk/pull/22)

## 1.4.1
  * Make some fields for the `sla_policies` stream nullable

## 1.4.0
  * Adds the SLA Policies stream [#21](https://github.com/singer-io/tap-zendesk/pull/21)

## 1.3.0
  * Add ability to auth with API token [#20](https://github.com/singer-io/tap-zendesk/pull/20)

## 1.2.2
  * The Following changes included in [#18](https://github.com/singer-io/tap-zendesk/pull/18):
      * Fixes a bug where some `group_membership` records were being returned without an `updated_at` field.  The solution is to check for the `updated_at`, and if it is not present, log an info message and sync it.  This means records without an `updated_at` field will be sync'd on every run.
      * Fixes schema refs by adding `shared/`
  * Add `report_csv` to `users` stream

## 1.2.1
  * Include shared schema refs in package

## 1.2.0
  * Add `satisfaction_ratings` and `ticket_comment` streams [#17](https://github.com/singer-io/tap-zendesk/pull/17)

## 1.1.1
  * Adds more JSON Schema to the tickets and ticket_audits schemas [#16](https://github.com/singer-io/tap-zendesk/pull/16)

## 1.1.0
  * Add `ticket_forms` stream [#15](https://github.com/singer-io/tap-zendesk/pull/15)

## 1.0.0
  * Version bump for initial release

## 0.4.3
  * Catch RecordNotFound exceptions in ticket_audits and ticket_metrics

## 0.4.2
  * Bug fix to populate tickets bookmark if replication_key changes

## 0.4.0
  * The following changes are included in [#14](https://github.com/singer-io/tap-zendesk/pull/14):
      * Made ticket_audits and ticket_metrics substreams of tickets stream.  This allows audits and metrics to be retrieved for archived tickets
      * Changed replication key to `generated_timestamp` for tickets, ticket_audits, and ticket_metrics
      * Added `problem_id`, `forum_topic_id`, and `satisfaction_rating` fields to tickets schema (addresses [#10](https://github.com/singer-io/tap-zendesk/issues/10))
      * Added `system_field_options` and `sub_type_id` to ticket_fields schema (addresses [#11](https://github.com/singer-io/tap-zendesk/issues/11))

## 0.3.2
  * Adding additional fields to the tickets schema [#13](https://github.com/singer-io/tap-zendesk/pull/13)

## 0.3.1
  * Bumped `singer-python` to 5.1.5 to get fix for empty `properties` subschema
  * Added `deleted_at` to organizations schema

## 0.3.0
  * Changed the '-' to a '_' in stream names [#9](https://github.com/singer-io/tap-zendesk/pull/9)

## 0.2.1
  * Fixed a bug in metadata generation for the Tags stream [#8](https://github.com/singer-io/tap-zendesk/pull/8)

## 0.2.0
  * Sets replication-keys to 'automatic' inclusion
  * Removes "fields" because they duplicate "custom-fields"
  * Skips tickets that come back by accident (out of the date range)
  * Updates to schemas [#7](https://github.com/singer-io/tap-zendesk/pull/7)

## 0.1.1
  * Update streams' schemas to reflect testing data
  * Add discovery of custom fields for organizations and users (fixes error when syncing these streams due to incorrect schema)

## 0.1.0
  * Add audits stream
  * Update schemas for other streams
  * Misc bug fixes

## 0.0.1
  * Initial end-to-end with most streams, bookmarking, and discovery
