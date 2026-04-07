"""
Gmail Multi-Account MCP Server
-------------------------------
Exposes Gmail operations for multiple Google accounts via the
Model Context Protocol (MCP) stdio transport.

Start with:  python server.py
Configure accounts in config.json and authenticate with: python setup_auth.py
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

from auth import AuthManager
from config import get_accounts, get_attachments_dir, get_client_secret_path, get_credentials_dir, load_config
from gcalendar import CalendarService
from gmail import GmailService

# ---------------------------------------------------------------------------
# Bootstrap: load config and auth manager at startup
# ---------------------------------------------------------------------------

try:
    _config = load_config()
    _accounts = get_accounts(_config)
    _credentials_dir = get_credentials_dir(_config)
    _client_secret_path = get_client_secret_path(_config)
    _attachments_dir = get_attachments_dir(_config)
    _auth = AuthManager(_credentials_dir, _client_secret_path)
except FileNotFoundError as exc:
    print(f"STARTUP ERROR: {exc}", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_creds(account_name: str):
    """Return valid credentials for an account or raise ValueError."""
    if account_name not in _accounts:
        raise ValueError(
            f"Unknown account '{account_name}'. Available: {list(_accounts.keys())}"
        )
    creds = _auth.get_credentials(account_name)
    if creds is None:
        email = _accounts[account_name].get("email", account_name)
        raise ValueError(
            f"Account '{account_name}' ({email}) is not authenticated. "
            "Run 'python setup_auth.py' to authenticate."
        )
    return creds


def _get_service(account_name: str) -> GmailService:
    return GmailService(_get_creds(account_name), account_name)


def _get_calendar(account_name: str) -> CalendarService:
    return CalendarService(_get_creds(account_name), account_name)


def _fmt(data: Any) -> list[types.TextContent]:
    if isinstance(data, str):
        return [types.TextContent(type="text", text=data)]
    return [types.TextContent(type="text", text=json.dumps(data, indent=2, ensure_ascii=False))]


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

server = Server("gmail-multi-account")


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="list_accounts",
            description=(
                "List all Gmail accounts configured in this MCP server, "
                "along with their authentication status."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="gmail_get_profile",
            description="Get the Gmail profile (email address, message count, thread count) for an account.",
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {
                        "type": "string",
                        "description": "Account name as defined in config.json (e.g. 'personal', 'work')",
                    }
                },
                "required": ["account"],
            },
        ),
        types.Tool(
            name="gmail_search",
            description=(
                "Search emails using Gmail search syntax. "
                "Searches a single account or all accounts if 'account' is omitted. "
                "Example queries: 'from:boss@company.com is:unread', 'subject:invoice has:attachment'"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {
                        "type": "string",
                        "description": "Account to search. Omit to search all configured accounts.",
                    },
                    "query": {
                        "type": "string",
                        "description": "Gmail search query string",
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Max results per account (default 10, max 50)",
                        "default": 10,
                    },
                    "include_body": {
                        "type": "boolean",
                        "description": "Include full message body in results (slower). Default: false.",
                        "default": False,
                    },
                },
                "required": ["query"],
            },
        ),
        types.Tool(
            name="gmail_read_message",
            description="Read the full content of a Gmail message by its ID.",
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {
                        "type": "string",
                        "description": "Account that owns the message",
                    },
                    "message_id": {
                        "type": "string",
                        "description": "Gmail message ID (from search results)",
                    },
                },
                "required": ["account", "message_id"],
            },
        ),
        types.Tool(
            name="gmail_read_thread",
            description="Read all messages in a Gmail thread/conversation.",
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {
                        "type": "string",
                        "description": "Account that owns the thread",
                    },
                    "thread_id": {
                        "type": "string",
                        "description": "Gmail thread ID",
                    },
                },
                "required": ["account", "thread_id"],
            },
        ),
        types.Tool(
            name="gmail_get_attachment",
            description=(
                "Download an attachment from a Gmail message. "
                "Use gmail_read_message first to get the attachment IDs and filenames."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {
                        "type": "string",
                        "description": "Account that owns the message",
                    },
                    "message_id": {
                        "type": "string",
                        "description": "Gmail message ID",
                    },
                    "attachment_id": {
                        "type": "string",
                        "description": "Attachment ID (from the attachments list in the message)",
                    },
                    "filename": {
                        "type": "string",
                        "description": "Filename for the saved attachment (from the attachments list)",
                    },
                },
                "required": ["account", "message_id", "attachment_id", "filename"],
            },
        ),
        types.Tool(
            name="gmail_reply",
            description=(
                "Reply to an existing email within its thread. "
                "Automatically sets the correct recipient, subject, and threading headers. "
                "Use gmail_read_message or gmail_read_thread first to get the message_id to reply to."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {
                        "type": "string",
                        "description": "Account to reply from",
                    },
                    "message_id": {
                        "type": "string",
                        "description": "Gmail message ID of the message to reply to",
                    },
                    "body": {
                        "type": "string",
                        "description": "Reply body. Supports HTML formatting. A plain text fallback is generated automatically.",
                    },
                    "cc": {"type": "string", "description": "CC recipients, comma-separated"},
                    "bcc": {"type": "string", "description": "BCC recipients, comma-separated"},
                    "attachments": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of absolute file paths to attach",
                    },
                    "draft": {
                        "type": "boolean",
                        "default": False,
                        "description": "If true, save as a draft reply in the correct thread instead of sending. Use this when the user has not explicitly approved sending yet.",
                    },
                },
                "required": ["account", "message_id", "body"],
            },
        ),
        types.Tool(
            name="gmail_forward",
            description=(
                "Forward a Gmail message to another recipient. "
                "Includes the original message body and all attachments. "
                "Optionally add your own message above the forwarded content."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {
                        "type": "string",
                        "description": "Account that owns the message",
                    },
                    "message_id": {
                        "type": "string",
                        "description": "Gmail message ID of the message to forward",
                    },
                    "to": {
                        "type": "string",
                        "description": "Recipient(s) to forward to, comma-separated",
                    },
                    "body": {
                        "type": "string",
                        "description": "Optional message to include above the forwarded content. Supports HTML.",
                        "default": "",
                    },
                    "cc": {"type": "string", "description": "CC recipients, comma-separated"},
                    "bcc": {"type": "string", "description": "BCC recipients, comma-separated"},
                    "draft": {
                        "type": "boolean",
                        "default": False,
                        "description": "If true, save as a draft instead of sending. Use this when the user has not explicitly approved sending yet.",
                    },
                },
                "required": ["account", "message_id", "to"],
            },
        ),
        types.Tool(
            name="gmail_send",
            description="Send a new email from a specific Gmail account.",
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {
                        "type": "string",
                        "description": "Account to send from",
                    },
                    "to": {
                        "type": "string",
                        "description": "Recipient(s), comma-separated",
                    },
                    "subject": {"type": "string", "description": "Email subject"},
                    "body": {"type": "string", "description": "Email body. Supports HTML formatting (e.g. <b>, <p>, <ul>, <a>). A plain text fallback is generated automatically."},
                    "cc": {"type": "string", "description": "CC recipients, comma-separated"},
                    "bcc": {"type": "string", "description": "BCC recipients, comma-separated"},
                    "attachments": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of absolute file paths to attach",
                    },
                    "draft": {
                        "type": "boolean",
                        "default": False,
                        "description": "If true, save as a draft instead of sending. Use this when the user has not explicitly approved sending yet.",
                    },
                    "body_type": {
                        "type": "string",
                        "enum": ["html", "plain"],
                        "default": "html",
                        "description": "Body content type. Defaults to 'html'. Use 'plain' only for explicit plain-text sends.",
                    },
                },
                "required": ["account", "to", "subject", "body"],
            },
        ),
        types.Tool(
            name="gmail_create_draft",
            description="Save an email as a draft in a specific Gmail account.",
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {
                        "type": "string",
                        "description": "Account to create the draft in",
                    },
                    "to": {"type": "string", "description": "Recipient(s), comma-separated"},
                    "subject": {"type": "string", "description": "Email subject"},
                    "body": {"type": "string", "description": "Email body. Supports HTML formatting (e.g. <b>, <p>, <ul>, <a>). A plain text fallback is generated automatically."},
                    "cc": {"type": "string", "description": "CC recipients"},
                    "bcc": {"type": "string", "description": "BCC recipients"},
                    "attachments": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of absolute file paths to attach",
                    },
                    "body_type": {
                        "type": "string",
                        "enum": ["html", "plain"],
                        "default": "html",
                        "description": "Body content type. Defaults to 'html'. Use 'plain' only for explicit plain-text drafts.",
                    },
                },
                "required": ["account", "to", "subject", "body"],
            },
        ),
        types.Tool(
            name="gmail_list_drafts",
            description="List draft emails in a Gmail account.",
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Account name"},
                    "max_results": {
                        "type": "integer",
                        "description": "Max drafts to return (default 10)",
                        "default": 10,
                    },
                },
                "required": ["account"],
            },
        ),
        types.Tool(
            name="gmail_list_labels",
            description="List all labels and folders in a Gmail account.",
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Account name"}
                },
                "required": ["account"],
            },
        ),
        types.Tool(
            name="gmail_create_label",
            description=(
                "Create a new label in a Gmail account. "
                "Supports nested labels using '/' as separator (e.g. 'Clients/Acme')."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Account name"},
                    "name": {
                        "type": "string",
                        "description": "Label name (e.g. 'Needs Response', 'Clients/Acme')",
                    },
                },
                "required": ["account", "name"],
            },
        ),
        types.Tool(
            name="gmail_modify_labels",
            description=(
                "Add or remove labels on a Gmail message. "
                "Common label IDs: STARRED, UNREAD, INBOX, SPAM, IMPORTANT."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Account name"},
                    "message_id": {"type": "string", "description": "Gmail message ID"},
                    "add_labels": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Label IDs to add (e.g. ['STARRED', 'UNREAD'])",
                    },
                    "remove_labels": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Label IDs to remove (e.g. ['UNREAD'])",
                    },
                },
                "required": ["account", "message_id"],
            },
        ),
        types.Tool(
            name="gmail_archive",
            description="Archive a Gmail message (remove it from the Inbox without deleting it).",
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Account name"},
                    "message_id": {"type": "string", "description": "Gmail message ID"},
                },
                "required": ["account", "message_id"],
            },
        ),
        types.Tool(
            name="gmail_modify_thread_labels",
            description=(
                "Add or remove labels on all messages in a Gmail thread at once. "
                "Common label IDs: STARRED, UNREAD, INBOX, SPAM, IMPORTANT."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Account name"},
                    "thread_id": {"type": "string", "description": "Gmail thread ID"},
                    "add_labels": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Label IDs to add to all messages in the thread",
                    },
                    "remove_labels": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Label IDs to remove from all messages in the thread",
                    },
                },
                "required": ["account", "thread_id"],
            },
        ),
        types.Tool(
            name="gmail_archive_thread",
            description="Archive an entire Gmail thread (remove all messages from the Inbox without deleting them).",
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Account name"},
                    "thread_id": {"type": "string", "description": "Gmail thread ID"},
                },
                "required": ["account", "thread_id"],
            },
        ),
        # ── Calendar tools ──────────────────────────────────────────────────
        types.Tool(
            name="calendar_list_calendars",
            description="List all Google Calendars available for an account (primary, work, shared, etc.).",
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Account name"},
                },
                "required": ["account"],
            },
        ),
        types.Tool(
            name="calendar_list_events",
            description=(
                "List upcoming calendar events for an account. "
                "Optionally filter by time range and calendar. "
                "Times must be in RFC3339 format, e.g. '2026-03-10T00:00:00Z'."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Account name"},
                    "calendar_id": {
                        "type": "string",
                        "description": "Calendar ID (default: 'primary'). Use calendar_list_calendars to get IDs.",
                        "default": "primary",
                    },
                    "time_min": {
                        "type": "string",
                        "description": "Start of range (RFC3339). Defaults to now.",
                    },
                    "time_max": {
                        "type": "string",
                        "description": "End of range (RFC3339). Optional.",
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Max events to return (default 20, max 50)",
                        "default": 20,
                    },
                },
                "required": ["account"],
            },
        ),
        types.Tool(
            name="calendar_search",
            description="Search for events by keyword across a calendar (title, description, location, attendees).",
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Account name"},
                    "query": {"type": "string", "description": "Search keyword(s)"},
                    "calendar_id": {
                        "type": "string",
                        "description": "Calendar ID (default: 'primary')",
                        "default": "primary",
                    },
                    "time_min": {
                        "type": "string",
                        "description": "Start of range (RFC3339). Defaults to now.",
                    },
                    "time_max": {
                        "type": "string",
                        "description": "End of range (RFC3339). Optional.",
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Max results (default 20)",
                        "default": 20,
                    },
                },
                "required": ["account", "query"],
            },
        ),
        types.Tool(
            name="calendar_get_event",
            description="Get full details of a specific calendar event by its ID.",
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Account name"},
                    "event_id": {"type": "string", "description": "Event ID (from list or search results)"},
                    "calendar_id": {
                        "type": "string",
                        "description": "Calendar ID (default: 'primary')",
                        "default": "primary",
                    },
                },
                "required": ["account", "event_id"],
            },
        ),
        types.Tool(
            name="calendar_create_event",
            description=(
                "Create a new calendar event. "
                "For timed events, use RFC3339 format (e.g. '2026-04-10T14:00:00-05:00'). "
                "For all-day events, set all_day to true and use date format (e.g. '2026-04-10')."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Account name"},
                    "summary": {"type": "string", "description": "Event title"},
                    "start": {
                        "type": "string",
                        "description": "Start time (RFC3339) or date (YYYY-MM-DD for all-day events)",
                    },
                    "end": {
                        "type": "string",
                        "description": "End time (RFC3339) or date (YYYY-MM-DD for all-day events)",
                    },
                    "description": {"type": "string", "description": "Event description"},
                    "location": {"type": "string", "description": "Event location"},
                    "attendees": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of attendee email addresses. Invitations are sent automatically.",
                    },
                    "calendar_id": {
                        "type": "string",
                        "description": "Calendar ID (default: 'primary')",
                        "default": "primary",
                    },
                    "all_day": {
                        "type": "boolean",
                        "description": "Set to true for all-day events (default: false)",
                        "default": False,
                    },
                },
                "required": ["account", "summary", "start", "end"],
            },
        ),
        types.Tool(
            name="calendar_update_event",
            description=(
                "Update an existing calendar event. "
                "Only the fields you provide will be changed; others remain as-is. "
                "Attendee updates notify all participants."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Account name"},
                    "event_id": {"type": "string", "description": "Event ID to update"},
                    "summary": {"type": "string", "description": "New event title"},
                    "start": {"type": "string", "description": "New start time (RFC3339) or date"},
                    "end": {"type": "string", "description": "New end time (RFC3339) or date"},
                    "description": {"type": "string", "description": "New event description"},
                    "location": {"type": "string", "description": "New event location"},
                    "attendees": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Updated list of attendee email addresses (replaces existing list)",
                    },
                    "calendar_id": {
                        "type": "string",
                        "description": "Calendar ID (default: 'primary')",
                        "default": "primary",
                    },
                },
                "required": ["account", "event_id"],
            },
        ),
        types.Tool(
            name="calendar_delete_event",
            description="Delete a calendar event. Cancellation notices are sent to all attendees.",
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Account name"},
                    "event_id": {"type": "string", "description": "Event ID to delete"},
                    "calendar_id": {
                        "type": "string",
                        "description": "Calendar ID (default: 'primary')",
                        "default": "primary",
                    },
                },
                "required": ["account", "event_id"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict | None) -> list[types.TextContent]:
    args = arguments or {}

    try:
        # ---- list_accounts ------------------------------------------------
        if name == "list_accounts":
            result = []
            for acct, info in _accounts.items():
                authenticated = _auth.is_authenticated(acct)
                result.append({
                    "name": acct,
                    "email": info.get("email", ""),
                    "description": info.get("description", ""),
                    "authenticated": authenticated,
                    "status": "ready" if authenticated else "not authenticated — run setup_auth.py",
                })
            return _fmt(result)

        # ---- gmail_get_profile --------------------------------------------
        elif name == "gmail_get_profile":
            svc = _get_service(args["account"])
            return _fmt(svc.get_profile())

        # ---- gmail_search -------------------------------------------------
        elif name == "gmail_search":
            query: str = args["query"]
            max_results: int = int(args.get("max_results", 10))
            include_body: bool = bool(args.get("include_body", False))
            account: str | None = args.get("account")

            if account:
                svc = _get_service(account)
                data = svc.search_messages(query, max_results, include_body=include_body)
                data["account"] = account
                data["email"] = _accounts[account].get("email", "")
                return _fmt(data)
            else:
                all_results = []
                for acct in _accounts:
                    try:
                        svc = _get_service(acct)
                        data = svc.search_messages(query, max_results, include_body=include_body)
                        all_results.append({
                            "account": acct,
                            "email": _accounts[acct].get("email", ""),
                            **data,
                        })
                    except ValueError as exc:
                        all_results.append({
                            "account": acct,
                            "error": str(exc),
                            "messages": [],
                        })
                return _fmt(all_results)

        # ---- gmail_read_message -------------------------------------------
        elif name == "gmail_read_message":
            svc = _get_service(args["account"])
            return _fmt(svc.get_message(args["message_id"]))

        # ---- gmail_read_thread --------------------------------------------
        elif name == "gmail_read_thread":
            svc = _get_service(args["account"])
            return _fmt(svc.get_thread(args["thread_id"]))

        # ---- gmail_get_attachment -----------------------------------------
        elif name == "gmail_get_attachment":
            svc = _get_service(args["account"])
            save_path = svc.get_attachment(
                message_id=args["message_id"],
                attachment_id=args["attachment_id"],
                filename=args["filename"],
                save_dir=_attachments_dir,
            )
            return _fmt({
                "status": "downloaded",
                "path": str(save_path),
                "filename": save_path.name,
            })

        # ---- gmail_reply --------------------------------------------------
        elif name == "gmail_reply":
            svc = _get_service(args["account"])
            is_draft = bool(args.get("draft", False))
            result = svc.reply(
                message_id=args["message_id"],
                body=args["body"],
                cc=args.get("cc", ""),
                bcc=args.get("bcc", ""),
                attachment_paths=args.get("attachments"),
                draft=is_draft,
            )
            if is_draft:
                return _fmt({
                    "status": "draft_saved",
                    "draft_id": result.get("id"),
                    "message_id": result.get("message", {}).get("id"),
                    "thread_id": result.get("message", {}).get("threadId"),
                })
            return _fmt({
                "status": "replied",
                "message_id": result.get("id"),
                "thread_id": result.get("threadId"),
            })

        # ---- gmail_forward ------------------------------------------------
        elif name == "gmail_forward":
            svc = _get_service(args["account"])
            is_draft = bool(args.get("draft", False))
            result = svc.forward(
                message_id=args["message_id"],
                to=args["to"],
                body=args.get("body", ""),
                cc=args.get("cc", ""),
                bcc=args.get("bcc", ""),
                draft=is_draft,
            )
            if is_draft:
                return _fmt({
                    "status": "draft_saved",
                    "draft_id": result.get("id"),
                    "message_id": result.get("message", {}).get("id"),
                    "thread_id": result.get("message", {}).get("threadId"),
                })
            return _fmt({
                "status": "forwarded",
                "message_id": result.get("id"),
                "thread_id": result.get("threadId"),
            })

        # ---- gmail_send ---------------------------------------------------
        elif name == "gmail_send":
            svc = _get_service(args["account"])
            is_draft = bool(args.get("draft", False))
            result = svc.send_message(
                to=args["to"],
                subject=args["subject"],
                body=args["body"],
                cc=args.get("cc", ""),
                bcc=args.get("bcc", ""),
                attachment_paths=args.get("attachments"),
                draft=is_draft,
                body_type=args.get("body_type", "html"),
            )
            if is_draft:
                return _fmt({
                    "status": "draft_saved",
                    "draft_id": result.get("id"),
                    "message_id": result.get("message", {}).get("id"),
                    "thread_id": result.get("message", {}).get("threadId"),
                })
            return _fmt({
                "status": "sent",
                "message_id": result.get("id"),
                "thread_id": result.get("threadId"),
            })

        # ---- gmail_create_draft -------------------------------------------
        elif name == "gmail_create_draft":
            svc = _get_service(args["account"])
            result = svc.create_draft(
                to=args["to"],
                subject=args["subject"],
                body=args["body"],
                cc=args.get("cc", ""),
                bcc=args.get("bcc", ""),
                attachment_paths=args.get("attachments"),
                body_type=args.get("body_type", "html"),
            )
            return _fmt({"status": "draft created", "draft_id": result.get("id")})

        # ---- gmail_list_drafts --------------------------------------------
        elif name == "gmail_list_drafts":
            svc = _get_service(args["account"])
            drafts = svc.list_drafts(int(args.get("max_results", 10)))
            return _fmt({"count": len(drafts), "drafts": drafts})

        # ---- gmail_list_labels --------------------------------------------
        elif name == "gmail_list_labels":
            svc = _get_service(args["account"])
            return _fmt(svc.list_labels())

        # ---- gmail_create_label -------------------------------------------
        elif name == "gmail_create_label":
            svc = _get_service(args["account"])
            label = svc.create_label(args["name"])
            return _fmt({"status": "label created", **label})

        # ---- gmail_modify_labels -----------------------------------------
        elif name == "gmail_modify_labels":
            svc = _get_service(args["account"])
            svc.modify_labels(
                message_id=args["message_id"],
                add_labels=args.get("add_labels"),
                remove_labels=args.get("remove_labels"),
            )
            return _fmt({"status": "labels updated", "message_id": args["message_id"]})

        # ---- gmail_archive -----------------------------------------------
        elif name == "gmail_archive":
            svc = _get_service(args["account"])
            svc.archive_message(args["message_id"])
            return _fmt({"status": "archived", "message_id": args["message_id"]})

        # ---- gmail_modify_thread_labels -----------------------------------
        elif name == "gmail_modify_thread_labels":
            svc = _get_service(args["account"])
            svc.modify_thread_labels(
                thread_id=args["thread_id"],
                add_labels=args.get("add_labels"),
                remove_labels=args.get("remove_labels"),
            )
            return _fmt({"status": "thread labels updated", "thread_id": args["thread_id"]})

        # ---- gmail_archive_thread -----------------------------------------
        elif name == "gmail_archive_thread":
            svc = _get_service(args["account"])
            svc.archive_thread(args["thread_id"])
            return _fmt({"status": "thread archived", "thread_id": args["thread_id"]})

        # ---- calendar_list_calendars --------------------------------------
        elif name == "calendar_list_calendars":
            svc = _get_calendar(args["account"])
            return _fmt(svc.list_calendars())

        # ---- calendar_list_events -----------------------------------------
        elif name == "calendar_list_events":
            svc = _get_calendar(args["account"])
            return _fmt(svc.list_events(
                time_min=args.get("time_min"),
                time_max=args.get("time_max"),
                max_results=int(args.get("max_results", 20)),
                calendar_id=args.get("calendar_id", "primary"),
            ))

        # ---- calendar_search ----------------------------------------------
        elif name == "calendar_search":
            svc = _get_calendar(args["account"])
            return _fmt(svc.search_events(
                query=args["query"],
                time_min=args.get("time_min"),
                time_max=args.get("time_max"),
                max_results=int(args.get("max_results", 20)),
                calendar_id=args.get("calendar_id", "primary"),
            ))

        # ---- calendar_get_event -------------------------------------------
        elif name == "calendar_get_event":
            svc = _get_calendar(args["account"])
            return _fmt(svc.get_event(
                event_id=args["event_id"],
                calendar_id=args.get("calendar_id", "primary"),
            ))

        # ---- calendar_create_event ----------------------------------------
        elif name == "calendar_create_event":
            svc = _get_calendar(args["account"])
            event = svc.create_event(
                summary=args["summary"],
                start=args["start"],
                end=args["end"],
                description=args.get("description", ""),
                location=args.get("location", ""),
                attendees=args.get("attendees"),
                calendar_id=args.get("calendar_id", "primary"),
                all_day=bool(args.get("all_day", False)),
            )
            return _fmt({"status": "event created", **event})

        # ---- calendar_update_event ----------------------------------------
        elif name == "calendar_update_event":
            svc = _get_calendar(args["account"])
            event = svc.update_event(
                event_id=args["event_id"],
                calendar_id=args.get("calendar_id", "primary"),
                summary=args.get("summary"),
                start=args.get("start"),
                end=args.get("end"),
                description=args.get("description"),
                location=args.get("location"),
                attendees=args.get("attendees"),
            )
            return _fmt({"status": "event updated", **event})

        # ---- calendar_delete_event ----------------------------------------
        elif name == "calendar_delete_event":
            svc = _get_calendar(args["account"])
            svc.delete_event(
                event_id=args["event_id"],
                calendar_id=args.get("calendar_id", "primary"),
            )
            return _fmt({"status": "event deleted", "event_id": args["event_id"]})

        else:
            return _fmt(f"Unknown tool: {name}")

    except ValueError as exc:
        return _fmt(f"Error: {exc}")
    except Exception as exc:
        return _fmt(f"Error in '{name}': {type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(main())
