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

### Recurring full scans and large-account acceptance

**Every sync scans all available pages of `users`, `organizations`, and `tickets`
when those streams are selected or traversed for selected children.** These are
incremental outputs, not incremental API reads: bookmarks filter records locally
after retrieval. An unchanged account still incurs the full list scan. A recent
`start_date`, a later bookmark, or selecting only a child does not eliminate that
parent scan. Tickets here means the non-archived tickets visible to the credentials.

The [List Users](https://developer.zendesk.com/api-reference/ticketing/users/users/#list-users),
[List Organizations](https://developer.zendesk.com/api-reference/ticketing/organizations/organizations/#list-organizations),
and [List Tickets](https://developer.zendesk.com/api-reference/ticketing/tickets/tickets/#list-tickets)
calls used by this tap do not supply a server-side bookmark/date filter.
Sorting by `updated_at` and checking cursor progress do not restrict retrieval to
changed records. This migration does not provide a delta-read replacement for the
retired Incremental APIs. This is not a claim that Zendesk has no other APIs:
[Search and Search Export](https://developer.zendesk.com/api-reference/ticketing/ticket-management/search/)
are different interfaces, with search-indexing delays and their own limits; they
are not implemented or validated as equivalent delta sources by this tap.

#### Request and runtime budget

With page size `P` (default **100**; these list endpoints support at most 100 records
per page), budget approximately `max(1, ceil(N / P))` list requests **per stream,
per sync**, where `N` is the number of records available from that endpoint.
Sum this across the parent streams being traversed. Empty/final pages, discovery,
retries, other streams, and child requests can add to this estimate.

| Available records in one parent stream | List requests per sync at page size 100 |
| --- | --- |
| 10,000 | 100 |
| 100,000 | 1,000 |
| 1,000,000 | 10,000 |

Parent pages are fetched sequentially. For planning, a lower-bound estimate is
`max(list_requests * average_page_latency_seconds, 60 * list_requests / available_requests_per_minute)`.
Use the effective request budget remaining after other integrations and applicable
endpoint limits, not just the account's advertised maximum. For example, 10,000
list requests at 200 ms per page require about 33 minutes of request latency;
with only 200 requests/minute available, the rate-limit floor is 50 minutes.
These are illustrative calculations, **not measured runtime or an SLA**. Record
processing, child requests, throttling, and retry backoff increase elapsed time.
Concurrent child fetching does not remove the sequential parent-page scan.

Selected children add work for parents at or after their applicable bookmark
(including boundary replays): metric events add one Show Ticket request per
eligible ticket; audits/comments share paginated audit requests; user identities,
user attribute values, and side conversations add their own requests and pages.
Ticket metric sets are side-loaded, so do not require a separate request.
Interrupted scans restart parent pagination; ticket bookmarks are committed only
after traversal and child fetching complete. Budget for repeated scans and
idempotent downstream upserts, not persisted mid-scan cursor resumption.

#### Validation and customer acceptance

The offline regression tests in `test/unittests/test_ticket_migration.py` validate
the request-count model against simulated HTTP responses for **10,001 records
(101 pages at the default page size) for each of the three parent streams**. They
check repeated scans with all records older than the bookmark, a qualifying record
on the final page, bookmark-boundary replay on the next unchanged sync, and empty
and exact-page-size datasets:

```sh
python -m pytest -q test/unittests/test_ticket_migration.py -k TestFullAccountScans
```

These tests validate pagination/filtering behavior, **not live Zendesk throughput,
rate-limit capacity, or large-account production readiness**. Before accepting
this migration for a large account:

1. Run a representative initial sync and at least one unchanged follow-up sync
   using the intended stream/child selection, page size, credentials, and schedule.
2. Record account record counts, list and child request counts, emitted records,
   total elapsed time, HTTP 429s, retry/backoff time, and the API capacity shared
   with other integrations. Include bookmark-boundary replays in the measurement.
3. Exercise interruption/restart and verify that replayed records are upserted
   safely and that a complete scan fits the operational window.
4. Agree on a sync interval and API budget with headroom for account growth and
   retries; avoid overlapping runs. If a full scan cannot fit, do not represent
   this migration as meeting the customer's freshness requirement.
5. Obtain explicit customer acceptance of recurring full-account scans, measured
   runtime/API usage, replay behavior, and the archived-ticket limitation above.
   If delta reads are required, confirm an eligible alternative with Zendesk and
   validate it separately before deployment; this tap has no delta fallback.

Live-account measurements and customer sign-off remain deployment prerequisites;
the offline tests do not substitute for them.

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
