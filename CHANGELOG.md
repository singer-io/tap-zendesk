# Changelog

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
