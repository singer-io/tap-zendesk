from tap_zendesk.streams.activities import Activities
from tap_zendesk.streams.audit_logs import AuditLogs
from tap_zendesk.streams.automations import Automations
from tap_zendesk.streams.bookmarks import Bookmarks
from tap_zendesk.streams.brands import Brands
from tap_zendesk.streams.custom_objects import CustomObjects
from tap_zendesk.streams.custom_roles import CustomRoles
from tap_zendesk.streams.deleted_tickets import DeletedTickets
from tap_zendesk.streams.deleted_users import DeletedUsers
from tap_zendesk.streams.dynamic_content_items import DynamicContentItems
from tap_zendesk.streams.groups import Groups
from tap_zendesk.streams.group_memberships import GroupMemberships
from tap_zendesk.streams.macros import Macros
from tap_zendesk.streams.organizations import Organizations
from tap_zendesk.streams.satisfaction_ratings import SatisfactionRatings
from tap_zendesk.streams.sessions import Sessions
from tap_zendesk.streams.sharing_agreements import SharingAgreements
from tap_zendesk.streams.side_conversations_events import SideConversationsEvents
from tap_zendesk.streams.sla_policies import SLAPolicies
from tap_zendesk.streams.recipient_addresses import RecipientAddresses
from tap_zendesk.streams.suspended_tickets import SuspendedTickets
from tap_zendesk.streams.tags import Tags
from tap_zendesk.streams.talk_phone_numbers import TalkPhoneNumbers
from tap_zendesk.streams.target_failures import TargetFailures
from tap_zendesk.streams.targets import Targets
from tap_zendesk.streams.ticket_audits import TicketAudits
from tap_zendesk.streams.ticket_comments import TicketComments
from tap_zendesk.streams.ticket_fields import TicketFields
from tap_zendesk.streams.ticket_forms import TicketForms
from tap_zendesk.streams.ticket_metric_events import TicketMetricEvents
from tap_zendesk.streams.ticket_metrics import TicketMetrics
from tap_zendesk.streams.tickets import Tickets
from tap_zendesk.streams.users import Users
from tap_zendesk.streams.account_attribute_definitions import AccountAttributeDefinitions
from tap_zendesk.streams.account_attributes import AccountAttributes
from tap_zendesk.streams.locales import Locales
from tap_zendesk.streams.job_statuses import JobStatuses
from tap_zendesk.streams.macro_actions import MacroActions
from tap_zendesk.streams.macro_categories import MacroCategories
from tap_zendesk.streams.macro_definitions import MacroDefinitions
from tap_zendesk.streams.monitored_twitter_handles import MonitoredTwitterHandles
from tap_zendesk.streams.organization_memberships import OrganizationMemberships
from tap_zendesk.streams.organization_subscriptions import OrganizationSubscriptions
from tap_zendesk.streams.support_requests import SupportRequests
from tap_zendesk.streams.resource_collections import ResourceCollections
from tap_zendesk.streams.satisfaction_reasons import SatisfactionReasons
from tap_zendesk.streams.schedules import Schedules
from tap_zendesk.streams.trigger_categories import TriggerCategories
from tap_zendesk.streams.triggers import Triggers
from tap_zendesk.streams.views import Views
from tap_zendesk.streams.workspaces import Workspaces
from tap_zendesk.streams.incremental_ticket_events import IncrementalTicketEvents
from tap_zendesk.streams.trigger_revisions import TriggerRevisions
from tap_zendesk.streams.macro_attachments import MacroAttachments
from tap_zendesk.streams.ticket_skips import TicketSkips
from tap_zendesk.streams.user_attribute_values import UserAttributeValues
from tap_zendesk.streams.user_identities import UserIdentities
from tap_zendesk.streams.schedule_holidays import ScheduleHolidays
from tap_zendesk.streams.side_conversations import SideConversations


STREAMS = {
    "activities": Activities,
    "audit_logs": AuditLogs,
    "automations": Automations,
    "bookmarks": Bookmarks,
    "brands": Brands,
    "custom_objects": CustomObjects,
    "custom_roles": CustomRoles,
    "deleted_tickets": DeletedTickets,
    "deleted_users": DeletedUsers,
    "dynamic_content_items": DynamicContentItems,
    "groups": Groups,
    "group_memberships": GroupMemberships,
    "macros": Macros,
    "organizations": Organizations,
    "satisfaction_ratings": SatisfactionRatings,
    "sessions": Sessions,
    "sharing_agreements": SharingAgreements,
    "side_conversations_events": SideConversationsEvents,
    "sla_policies": SLAPolicies,
    "recipient_addresses": RecipientAddresses,
    "suspended_tickets": SuspendedTickets,
    "talk_phone_numbers": TalkPhoneNumbers,
    "tags": Tags,
    "targets": Targets,
    "target_failures": TargetFailures,
    "tickets": Tickets,
    "ticket_audits": TicketAudits,
    "ticket_comments": TicketComments,
    "ticket_fields": TicketFields,
    "ticket_forms": TicketForms,
    "ticket_metrics": TicketMetrics,
    "ticket_metric_events": TicketMetricEvents,
    "users": Users,
    "account_attribute_definitions": AccountAttributeDefinitions,
    "account_attributes": AccountAttributes,
    "locales": Locales,
    "job_statuses": JobStatuses,
    "macro_actions": MacroActions,
    "macro_categories": MacroCategories,
    "macro_definitions": MacroDefinitions,
    "monitored_twitter_handles": MonitoredTwitterHandles,
    "organization_memberships": OrganizationMemberships,
    "organization_subscriptions": OrganizationSubscriptions,
    "support_requests": SupportRequests,
    "resource_collections": ResourceCollections,
    "satisfaction_reasons": SatisfactionReasons,
    "schedules": Schedules,
    "triggers": Triggers,
    "trigger_categories": TriggerCategories,
    "views": Views,
    "workspaces": Workspaces,
    "incremental_ticket_events": IncrementalTicketEvents,
    "trigger_revisions": TriggerRevisions,
    "macro_attachments": MacroAttachments,
    "ticket_skips": TicketSkips,
    "user_attribute_values": UserAttributeValues,
    "user_identities": UserIdentities,
    "schedule_holidays": ScheduleHolidays,
    "side_conversations": SideConversations
}
