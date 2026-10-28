# tap-zendesk
Tap for Zendesk

## Installation

1. Create and activate a virtualenv
1. `pip install -e '.[dev]'`

---

## Upgrading to 2.10.0

Rediscover the catalog and reapply stream and field selections before upgrading
if the existing catalog contains `incremental_ticket_events`, even if that stream
is unselected. Sync rejects these catalogs with a migration error before emitting
records. The stream has been removed; `ticket_audits` provides ticket change
history but is not schema- or record-compatible. Consumers of
`incremental_ticket_events` must migrate their downstream models explicitly;
rediscovery alone does not preserve that stream's output.

Keep the existing state. `ticket_metric_events` remains an independent incremental
stream and resumes from its own `time` bookmark (or `start_date` when absent),
regardless of the `tickets.generated_timestamp` bookmark. It uses Zendesk's
[List Ticket Metric Events](https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_metric_events/#list-ticket-metric-events)
endpoint with cursor pagination, not ticket sideloads. Bookmark-boundary records
may be replayed and should be upserted by their record keys.

`tickets` uses the paginated
[List Tickets](https://developer.zendesk.com/api-reference/ticketing/tickets/tickets/#list-tickets)
endpoint. Zendesk excludes archived tickets from this endpoint; therefore fresh
syncs and ongoing syncs may omit archived tickets. This limitation also applies
to data fetched per ticket, such as ticket metrics, audits, comments, and side
conversations, when those records are not otherwise available. Metric events are
fetched independently using their own time bookmark, not side-loaded from tickets;
do not assume this restores complete archived-ticket coverage. Zendesk's
[Incremental Ticket Export](https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/#incremental-ticket-export-cursor-based)
is the documented option when full archived-ticket coverage is required, but this
release does not use it.

## Authentication

### Using OAuth

OAuth is the default authentication method for `tap-zendesk`. To use OAuth, you will need to fetch an `access_token` from a configured Zendesk integration. See https://support.zendesk.com/hc/en-us/articles/203663836 for more details on how to integrate your application with Zendesk.

**config.json**
```json
{
  "access_token": "AVERYLONGOAUTHTOKEN",
  "subdomain": "acme",
  "start_date": "2000-01-01T00:00:00Z",
  "request_timeout": 300
}
```
- `request_timeout` (integer, `300`): It is the time for which request should wait to get response. It is an optional parameter and default request_timeout is 300 seconds.
### Using API Tokens

For a simplified, but less granular setup, you can use the API Token authentication which can be generated from the Zendesk Admin page. See https://support.zendesk.com/hc/en-us/articles/226022787-Generating-a-new-API-token- for more details about generating an API Token. You'll then be able to use the admins's `email` and the generated `api_token` to authenticate.

**config.json**
```json
{
  "email": "user@domain.com",
  "api_token": "THISISAVERYLONGTOKEN",
  "subdomain": "acme",
  "start_date": "2000-01-01T00:00:00Z",
  "request_timeout": 300
}
```
- `request_timeout` (integer, `300`):It is the time for which request should wait to get response. It is an optional parameter and default request_timeout is 300 seconds.

Copyright &copy; 2018 Stitch
