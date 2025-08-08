# Followers Count Updater (No Twitter API)

This script updates the `followers_count` column in your CSV using a public Twitter syndication endpoint. It does not require Twitter API keys.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
python fetch_followers_counts.py /absolute/path/to/your.csv --output-csv /absolute/path/to/output.csv \
  --batch-size 100 --concurrency 20 --max-retries 3 --timeout-sec 15
```

- `--batch-size`: usernames per request (the endpoint accepts many at once; 50-100 is a good start)
- `--concurrency`: max concurrent requests
- `--max-retries`: retries on rate limits/transient errors
- `--timeout-sec`: per-request timeout
- `--username-column`: if your CSV uses a different column name than `username`

The script normalizes `@username` to `username` automatically.

The resulting CSV will have `followers_count` filled/updated for rows whose usernames were successfully found.