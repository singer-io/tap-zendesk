# tap-zendesk
Tap for Zendesk

## Installation

1. Create and activate a virtualenv
1. `pip install -e '.[dev]'`

---

## Upgrading to 2.10.0

Zendesk is retiring the Incremental APIs for third-party integrations, so the tap
now uses the paginated replacements listed in Zendesk's
[announcement](https://support.zendesk.com/hc/en-us/articles/11249701485338-Announcing-end-of-availability-of-Incremental-APIs-for-third-party-developers).

- **Archived tickets are not synced.** `tickets` uses
  [List Tickets](https://developer.zendesk.com/api-reference/ticketing/tickets/tickets/#list-tickets),
  which excludes archived tickets (typically closed for about 120 days). Zendesk has no
  non-incremental endpoint that includes them. Tickets, and their ticket metrics, metric
  events, audits, comments and side conversations, are captured only while the ticket is
  not yet archived, so a historical backfill of tickets older than that is incomplete.
- **`ticket_metric_events`** is fetched per ticket with
  `GET /api/v2/tickets/{id}?include=metric_events`, because Zendesk documents that
  sideload for a single ticket only, instead of the retired incremental endpoint. This
  costs about one extra request per ticket (batched like audits). It keeps its own `time`
  bookmark, so existing state works unchanged. When the `tickets` and
  `ticket_metric_events` bookmarks differ, tickets are walked from the earlier one so no
  events are skipped. Events at the bookmark boundary may be replayed; upsert by `id`.
- **Bookmarks are saved only after every ticket page has been read.** List Tickets is
  ordered by `updated_at` while the `tickets` bookmark is `generated_timestamp`, so a
  bookmark saved part way through could skip tickets after an interruption. An
  interrupted sync therefore starts the ticket walk again, and records may be replayed.
- **Both authentication methods work for every stream.** OAuth (`access_token`) is sent
  as a Bearer token and email + `api_token` as Basic authentication.
- **`incremental_ticket_events` is removed.** Zendesk recommends ticket audits instead;
  the `ticket_audits` stream already covers this but is not schema-compatible. A catalog
  or state that still contains `incremental_ticket_events` is ignored with a warning.
  Rediscover the catalog to remove it, and migrate any downstream models that used it.
- Cursor pagination now fails with an error if Zendesk reports more pages but returns a
  missing or repeated cursor, instead of looping forever.

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
