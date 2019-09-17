# Changelog

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
