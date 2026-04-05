# Gmail & Calendar Multi-Account MCP Server

A local [Model Context Protocol (MCP)](https://modelcontextprotocol.io) server that connects multiple Gmail accounts and Google Calendars to Claude Desktop. Runs entirely on your machine — no cloud hosting required.

## Features

- **Multiple accounts** — connect as many Gmail or Google Workspace accounts as you need
- **Unified email search** — search across all accounts simultaneously with Gmail's full query syntax
- **Full read access** — read individual messages and entire threads
- **Attachments** — download attachments from emails and attach files to outgoing messages and drafts
- **Send & draft** — compose and send emails, or save drafts, from any account
- **Label management** — list labels, mark as read/unread, star messages
- **Google Calendar** — list calendars, browse upcoming events, search by keyword

## Requirements

- macOS (tested on macOS 14+)
- Python 3.11+
- A Google Cloud project with the Gmail API and Calendar API enabled (free)
- Claude Desktop

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/DiegoMaldonadoRosas/gmail-mcp.git
cd gmail-mcp
```

### 2. Run the setup script

```bash
bash setup.sh
```

This creates a virtual environment and installs all Python dependencies.

### 3. Configure your accounts

Copy the example config and fill in your accounts:

```bash
cp config.json.example config.json
```

Edit `config.json`:

```json
{
  "accounts": {
    "personal": {
      "email": "you@gmail.com",
      "description": "Personal Gmail"
    },
    "work": {
      "email": "you@company.com",
      "description": "Work account"
    }
  },
  "credentials_dir": "./credentials"
}
```

The account keys (`personal`, `work`) are the names you'll use when asking Claude to interact with a specific account.

### 4. Get Google OAuth credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project and enable both the **Gmail API** and the **Google Calendar API**
3. Go to **APIs & Services → Credentials → + Create Credentials → OAuth 2.0 Client ID**
4. Choose **Desktop app** as the application type
5. Download the JSON file and save it as `credentials/client_secret.json`
6. Go to **APIs & Services → OAuth consent screen → Test users** and add every email address you configured in `config.json`

### 5. Authenticate your accounts

```bash
source .venv/bin/activate
python setup_auth.py
```

A browser window will open for each account. Sign in with the correct Google account. Tokens are saved locally and refreshed automatically — you only need to do this once per account.

> **Note:** If you previously authenticated for Gmail only, you must re-run `setup_auth.py` after adding Calendar support so the tokens include the new Calendar permissions.

### 6. Add the server to Claude Desktop

Open `~/Library/Application Support/Claude/claude_desktop_config.json` and add:

```json
{
  "mcpServers": {
    "gmail": {
      "command": "/absolute/path/to/gmail-mcp/.venv/bin/python",
      "args": ["/absolute/path/to/gmail-mcp/server.py"]
    }
  }
}
```

Replace `/absolute/path/to/gmail-mcp` with the actual path where you cloned the repo.

### 7. Restart Claude Desktop

All tools will appear automatically.

## Available Tools

### Gmail

| Tool | Description |
|------|-------------|
| `list_accounts` | List all configured accounts and their auth status |
| `gmail_get_profile` | Get account profile and mailbox stats |
| `gmail_search` | Search emails using Gmail query syntax (one or all accounts) |
| `gmail_read_message` | Read the full content of a message |
| `gmail_read_thread` | Read all messages in a thread |
| `gmail_get_attachment` | Download an attachment from a message |
| `gmail_reply` | Reply to a message within its thread |
| `gmail_forward` | Forward a message (with attachments) to another recipient |
| `gmail_send` | Send a new email (with optional attachments) |
| `gmail_create_draft` | Save a draft (with optional attachments) |
| `gmail_list_drafts` | List drafts in an account |
| `gmail_list_labels` | List all labels and folders |
| `gmail_create_label` | Create a new label (supports nesting, e.g. `Clients/Acme`) |
| `gmail_modify_labels` | Add or remove labels (mark read/unread, star, etc.) |
| `gmail_archive` | Archive a message (remove from Inbox) |
| `gmail_modify_thread_labels` | Add or remove labels on an entire thread at once |
| `gmail_archive_thread` | Archive an entire thread |

### Google Calendar

| Tool | Description |
|------|-------------|
| `calendar_list_calendars` | List all calendars for an account (primary, work, shared, etc.) |
| `calendar_list_events` | List upcoming events, optionally filtered by date range |
| `calendar_search` | Search events by keyword (title, description, location, attendees) |
| `calendar_get_event` | Get full details of a specific event |
| `calendar_create_event` | Create a new event (with optional attendees) |
| `calendar_update_event` | Update an existing event (partial updates supported) |
| `calendar_delete_event` | Delete an event (notifies attendees) |

## Usage Examples

Once connected, you can ask Claude things like:

**Email:**
- *"Do I have any unread emails in my work account?"*
- *"Search for invoices received in the last month across all my accounts"*
- *"Read the last email from John in my personal account"*
- *"Reply to that customer support email and let them know we're looking into it"*
- *"Forward that invoice to accounting@company.com"*
- *"Draft a reply to the budget email in my work account"*
- *"Mark all emails from newsletter@example.com as read"*

**Calendar:**
- *"What meetings do I have this week in my work account?"*
- *"Search for events related to 'product launch' in my personal calendar"*
- *"List all my calendars in my work account"*
- *"What are the details of tomorrow's standup?"*
- *"Schedule a 30-minute follow-up call with jane@client.com next Tuesday at 2pm"*
- *"Move the investor meeting to Thursday and add our CFO"*
- *"Cancel the team lunch on Friday"*

## Adding a New Account

1. Add the account to `config.json`
2. Add the email as a Test User in Google Cloud Console (OAuth consent screen)
3. Run `python setup_auth.py` — it will only prompt for the new account
4. Restart Claude Desktop

## Security

- OAuth tokens are stored locally in `credentials/tokens/` and are excluded from version control via `.gitignore`
- `config.json` (which contains your email addresses) is also excluded from version control
- Nothing is sent to any third-party server — all communication is directly between your Mac and Google's APIs
- To revoke access at any time, visit [myaccount.google.com/permissions](https://myaccount.google.com/permissions)

## Project Structure

```
gmail-mcp/
├── server.py           # MCP server — exposes 15 tools to Claude
├── auth.py             # OAuth2 token manager (per account)
├── gmail.py            # Gmail API wrapper
├── gcalendar.py        # Google Calendar API wrapper
├── config.py           # Configuration loader
├── setup_auth.py       # One-time authentication script
├── setup.sh            # First-time installer
├── requirements.txt    # Python dependencies
├── config.json.example # Account configuration template
└── .gitignore          # Excludes credentials and config.json
```

## License

MIT
