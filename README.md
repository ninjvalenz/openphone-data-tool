# OpenPhone Data Consolidation Tool

Fetches users, phone numbers, conversations, calls (with transcripts), and messages from the OpenPhone API and writes a single consolidated JSON file.

## Prerequisites

- Python 3.12+
- An OpenPhone API key ([get one here](https://app.openphone.com/settings/api))

## Setup

1. **Clone the repository**

   ```bash
   git clone https://github.com/your-username/openphone-data-tool.git
   cd openphone-data-tool
   ```

2. **Create a virtual environment**

   ```bash
   python -m venv .venv
   ```

3. **Activate the virtual environment**

   Windows (CMD):
   ```bash
   .venv\Scripts\activate
   ```

   Windows (PowerShell):
   ```bash
   .venv\Scripts\Activate.ps1
   ```

   macOS / Linux:
   ```bash
   source .venv/bin/activate
   ```

4. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

5. **Set up your API key**

   Copy the example env file and add your key:

   ```bash
   cp .env.example .env
   ```

   Then edit `.env` and replace `your_api_key_here` with your actual OpenPhone API key:

   ```
   OPENPHONE_API_KEY=your_actual_api_key
   ```

6. **Configure database connection (dialect-aware)**

   This project now uses a connection strategy based on `DATABASE_URL`, so the
   same call sites can support SQLite now and PostgreSQL/MSSQL later.

   SQLite example (current local setup):

   ```
   DATABASE_URL=sqlite:///D:/Development/OLJ-DB/mockOLJ/property_data.db
   ```

   PostgreSQL example (future):

   ```
   DATABASE_URL=postgresql+psycopg://user:password@localhost:5432/openphone_data
   ```

   MSSQL example (future):

   ```
   DATABASE_URL=mssql+pyodbc://user:password@localhost:1433/openphone_data?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
   ```

   Backwards compatibility:
   - `OLJ_DB_PATH` is still supported as a SQLite-only fallback.
   - If both are set, `DATABASE_URL` wins.

## Usage

**Fetch all users:**

```bash
python main.py
```

**Limit to a specific number of users:**

```bash
python main.py --max-count 5
```

**Custom output file path:**

```bash
python main.py --output my_data.json
```

**Custom failed items file path:**

```bash
python main.py --failed-output my_failed_items.json
```

**All options combined:**

```bash
python main.py --max-count 10 --output my_data.json --failed-output my_failed_items.json
```

## Database Connectivity Check

Run this before wiring write paths, to verify the configured database can be opened:

```bash
python -m jobs.check_database_connection
```

## Webhook Setup (Inbound SMS)

This project also includes a minimal webhook flow for inbound messages:
- Local endpoint path: `op_new_message`
- OpenPhone event subscribed: `message.received` only

1. Start the webhook receiver:

```bash
python events/op_new_message_receiver.py
```

By default it listens on `http://0.0.0.0:8080/op_new_message`.

2. Expose your local server publicly (for example with ngrok), then set:

```bash
OPENPHONE_WEBHOOK_BASE_URL=https://your-public-domain
OPENPHONE_WEBHOOK_SIGNING_SECRET_SMS=your_base64_signing_secret_for_sms_webhook
OPENPHONE_WEBHOOK_SIGNING_SECRET_CALLS=your_base64_signing_secret_for_calls_webhook
```

`OPENPHONE_WEBHOOK_SIGNING_SECRET_SMS` should be the `key` from your message webhook.
`OPENPHONE_WEBHOOK_SIGNING_SECRET_CALLS` is reserved for your calls receiver.
The receiver now verifies the `openphone-signature` header before processing events.
It also queues validated events and processes them asynchronously in worker threads.

Optional queue tuning env vars:
- `OPENPHONE_WEBHOOK_QUEUE_MAXSIZE` (default: `1000`)
- `OPENPHONE_WEBHOOK_WORKER_COUNT` (default: `2`)
- `OPENPHONE_WEBHOOK_ENQUEUE_TIMEOUT_SECONDS` (default: `1`)

3. Create (or reuse) the webhook in OpenPhone:

```bash
python -m jobs.setup_webhook --type message
```

Optional arguments:
- `--base-url` to override `OPENPHONE_WEBHOOK_BASE_URL`
- `--path` endpoint path override (default: `op_new_message`)
- `--label` webhook label (default: `op_new_message`)
- `--resource-ids` comma-separated phone number IDs (`PN...`) or `*`
- `--user-id` optional OpenPhone user ID
- `--delete-existing` delete matching webhook(s) first, then create/reuse
- `--delete-only` delete matching webhook(s) and exit

## Webhook Setup (Calls)

This project also includes a call webhook setup script:
- Local endpoint path: `op_new_calls`
- OpenPhone events subscribed (default): `call.ringing`, `call.completed`, `call.recording.completed`

Create (or reuse) the call webhook in OpenPhone:

```bash
python -m jobs.setup_webhook --type calls --base-url https://jaiden-eliminative-sparely.ngrok-free.dev
```

Optional arguments:
- `--base-url` to override `OPENPHONE_WEBHOOK_BASE_URL`
- `--path` endpoint path override (default: `op_new_calls`)
- `--label` webhook label (default: `op_new_calls`)
- `--events` comma-separated call events (defaults to all three above)
- `--resource-ids` comma-separated phone number IDs (`PN...`) or `*`
- `--user-id` optional OpenPhone user ID
- `--delete-existing` delete matching webhook(s) first, then create/reuse
- `--delete-only` delete matching webhook(s) and exit

## Output

The tool produces two files:

- **`consolidated_phone_data.json`** - The main output containing all fetched data, structured by user.
- **`failed_items.json`** - Only created if some items failed to fetch (e.g. server errors). Contains the parameters needed to retry each failed item.
