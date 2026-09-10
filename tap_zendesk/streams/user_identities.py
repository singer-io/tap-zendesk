import asyncio
from aiohttp import ClientSession
from tap_zendesk import http
from tap_zendesk.streams.abstracts import (
    PaginatedStream, ChildBookmarkMixin
)
from tap_zendesk.streams.users import UserSubStreamMixin
from tap_zendesk.exceptions import ZendeskNotFoundError

class UserIdentities(UserSubStreamMixin, ChildBookmarkMixin, PaginatedStream):
    name = "user_identities"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'users/{user_id}/identities'
    item_key = 'identities'
    pagination_type = "cursor"
    parent = "users"
    bookmark_value = None

    async def _fetch_raw_records(self, session, user_id):
        """
        Fetch all identity records for a single user. 404s (e.g. unverified
        or deleted users) are treated as "no identities" rather than an error.
        """
        url = self.get_stream_endpoint(parent_obj={"id": user_id})
        try:
            records = await http.paginate_cursor_async(
                session, url, self.config['access_token'], self.request_timeout,
                self.page_size, self.item_key
            )
        except ZendeskNotFoundError:
            records = []
        return user_id, records

    async def _fetch_batch(self, user_ids):
        async with ClientSession() as session:
            tasks = [self._fetch_raw_records(session, user_id) for user_id in user_ids]
            results = await asyncio.gather(*tasks)
        return dict(results)

    def sync_batch(self, state, user_records):
        """
        Concurrently fetch identities for a batch of parent user records,
        then apply the standard filtering/bookmarking logic (via
        `process_records`) in the original per-user order.
        """
        user_ids = [user_record["id"] for user_record in user_records]
        raw_records_by_user = asyncio.run(self._fetch_batch(user_ids))

        for user_record in user_records:
            raw_records = raw_records_by_user.get(user_record["id"], [])
            yield from self.process_records(state, raw_records, parent_obj=user_record)
