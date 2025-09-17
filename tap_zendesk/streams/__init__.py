from tap_zendesk.streams.groups import Groups
from tap_zendesk.streams.group_memberships import GroupMemberships
from tap_zendesk.streams.macros import Macros
from tap_zendesk.streams.organizations import Organizations
from tap_zendesk.streams.satisfaction_ratings import SatisfactionRatings
from tap_zendesk.streams.sla_policies import SLAPolicies
from tap_zendesk.streams.tags import Tags
from tap_zendesk.streams.talk_phone_numbers import TalkPhoneNumbers
from tap_zendesk.streams.ticket_audits import TicketAudits
from tap_zendesk.streams.ticket_comments import TicketComments
from tap_zendesk.streams.ticket_fields import TicketFields
from tap_zendesk.streams.ticket_forms import TicketForms
from tap_zendesk.streams.ticket_metric_events import TicketMetricEvents
from tap_zendesk.streams.ticket_metrics import TicketMetrics
from tap_zendesk.streams.tickets import Tickets
from tap_zendesk.streams.users import Users


STREAMS = {
    "groups": Groups,
    "group_memberships": GroupMemberships,
    "macros": Macros,
    "organizations": Organizations,
    "satisfaction_ratings": SatisfactionRatings,
    "sla_policies": SLAPolicies,
    "talk_phone_numbers": TalkPhoneNumbers,
    "tags": Tags,
    "tickets": Tickets,
    "ticket_audits": TicketAudits,
    "ticket_comments": TicketComments,
    "ticket_fields": TicketFields,
    "ticket_forms": TicketForms,
    "ticket_metrics": TicketMetrics,
    "ticket_metric_events": TicketMetricEvents,
    "users": Users
}
