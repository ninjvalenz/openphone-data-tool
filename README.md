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

## Output

The tool produces two files:

- **`consolidated_phone_data.json`** - The main output containing all fetched data, structured by user.
- **`failed_items.json`** - Only created if some items failed to fetch (e.g. server errors). Contains the parameters needed to retry each failed item.
