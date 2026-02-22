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

## Webhook Setup (Inbound SMS)

This project also includes a minimal webhook flow for inbound messages:
- Local endpoint path: `op_new_message`
- OpenPhone event subscribed: `message.received` only

1. Start the webhook receiver:

```bash
python events/openphone_new_message_receiver.py
```

By default it listens on `http://0.0.0.0:8080/op_new_message`.

2. Expose your local server publicly (for example with ngrok), then set:

```bash
OPENPHONE_WEBHOOK_BASE_URL=https://your-public-domain
OPENPHONE_WEBHOOK_SIGNING_SECRET=your_base64_signing_secret
```

`OPENPHONE_WEBHOOK_SIGNING_SECRET` should be the webhook signing key from OpenPhone/Quo.
The receiver now verifies the `openphone-signature` header before processing events.

3. Create (or reuse) the webhook in OpenPhone:

```bash
python -m jobs.setup_message_webhook
```

Optional arguments:
- `--base-url` to override `OPENPHONE_WEBHOOK_BASE_URL`
- `--label` webhook label (default: `op_new_message`)
- `--resource-ids` comma-separated phone number IDs (`PN...`) or `*`
- `--user-id` optional OpenPhone user ID

## Output

The tool produces two files:

- **`consolidated_phone_data.json`** - The main output containing all fetched data, structured by user.
- **`failed_items.json`** - Only created if some items failed to fetch (e.g. server errors). Contains the parameters needed to retry each failed item.
