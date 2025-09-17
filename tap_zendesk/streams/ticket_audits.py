import asyncio
from aiohttp import ClientSession
from tap_zendesk import http
from tap_zendesk import metrics as zendesk_metrics
from tap_zendesk.streams.abstracts import Stream, HEADERS


class TicketAudits(Stream):
    name = "ticket_audits"
    replication_method = "INCREMENTAL"
    count = 0
    endpoint='https://{}.zendesk.com/api/v2/tickets/{}/audits.json'
    item_key='audits'

    async def sync_in_bulk(self, ticket_ids, comments_stream):
        """
        Asynchronously fetch ticket audits for multiple tickets
        """
        # Create an asynchronous HTTP session
        async with ClientSession() as session:
            tasks = [self.sync(session, ticket_id, comments_stream)
                     for ticket_id in ticket_ids]
            # Run all tasks concurrently and wait for them to complete
            return await asyncio.gather(*tasks)

    async def get_objects(self, session, ticket_id):
        url = self.endpoint.format(self.config['subdomain'], ticket_id)
        # Fetch the ticket audits using pagination
        records = await http.paginate_ticket_audits(session, url, self.config['access_token'], self.request_timeout, self.page_size)

        return records[self.item_key]

    async def sync(self, session, ticket_id, comments_stream):
        """
        Fetch ticket audits for a single ticket. Also exctract comments for each audit.
        """
        audit_records, comment_records = [], []
        try:
            # Fetch ticket audits for the given ticket ID
            ticket_audits = await self.get_objects(session, ticket_id)
            for ticket_audit in ticket_audits:
                if self.is_selected():
                    zendesk_metrics.capture('ticket_audit')
                    self.count += 1
                    audit_records.append((self.stream, ticket_audit))

                if comments_stream.is_selected():
                    # Extract comments from ticket audit
                    ticket_comments = (
                        event for event in ticket_audit['events'] if event['type'] == 'Comment')
                    zendesk_metrics.capture('ticket_comments')
                    for ticket_comment in ticket_comments:
                        # Update the comment with additional information
                        ticket_comment.update({
                            'created_at': ticket_audit['created_at'],
                            'via': ticket_audit['via'],
                            'metadata': ticket_audit['metadata'],
                            'ticket_id': ticket_id
                        })

                        comments_stream.count += 1
                        comment_records.append(
                            (comments_stream.stream, ticket_comment))
        except http.ZendeskNotFoundError:
            return audit_records, comment_records

        return audit_records, comment_records

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''

        url = self.endpoint.format(self.config['subdomain'], '1')
        HEADERS['Authorization'] = 'Bearer {}'.format(self.config["access_token"])
        try:
            http.call_api(url, self.request_timeout, params={'per_page': 1}, headers=HEADERS)
        except http.ZendeskNotFoundError:
            #Skip 404 ZendeskNotFoundError error as goal is just to check whether TicketComments have read permission or not
            pass
