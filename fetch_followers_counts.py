#!/usr/bin/env python3

import asyncio
import argparse
import csv
import math
import sys
from typing import Dict, List, Tuple

import aiohttp
import pandas as pd


SYNDICATION_ENDPOINT = "https://cdn.syndication.twimg.com/widgets/followbutton/info.json"


def chunk_list(items: List[str], batch_size: int) -> List[List[str]]:
    if batch_size <= 0:
        return [items]
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def normalize_username(username: str) -> str:
    if not isinstance(username, str):
        return ""
    cleaned = username.strip()
    if cleaned.startswith("@"):
        cleaned = cleaned[1:]
    return cleaned


async def fetch_batch(
    session: aiohttp.ClientSession,
    usernames: List[str],
    max_retries: int,
    request_timeout_sec: float,
) -> Dict[str, int]:
    params = {"screen_names": ",".join(usernames)}
    attempt = 0
    backoff_sec = 1.0

    while True:
        try:
            async with session.get(SYNDICATION_ENDPOINT, params=params, timeout=aiohttp.ClientTimeout(total=request_timeout_sec)) as resp:
                if resp.status == 200:
                    # content_type can vary; allow aiohttp to infer
                    data = await resp.json(content_type=None)
                    result: Dict[str, int] = {}
                    if isinstance(data, list):
                        for item in data:
                            try:
                                sn = item.get("screen_name")
                                fc = item.get("followers_count")
                                if isinstance(sn, str) and isinstance(fc, int):
                                    result[sn.lower()] = fc
                            except Exception:
                                # skip any malformed item
                                continue
                    return result
                elif resp.status == 429:
                    # Rate limited: respect Retry-After if present
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after is not None:
                        try:
                            wait = float(retry_after)
                        except ValueError:
                            wait = backoff_sec
                    else:
                        wait = backoff_sec
                elif 500 <= resp.status < 600:
                    wait = backoff_sec
                else:
                    # Non-retryable status
                    return {}
        except (aiohttp.ClientError, asyncio.TimeoutError):
            wait = backoff_sec

        attempt += 1
        if attempt > max_retries:
            return {}

        await asyncio.sleep(wait)
        backoff_sec = min(backoff_sec * 2.0, 30.0)


async def gather_followers_counts(
    usernames: List[str],
    concurrency: int,
    batch_size: int,
    max_retries: int,
    request_timeout_sec: float,
) -> Dict[str, int]:
    connector = aiohttp.TCPConnector(limit=concurrency)
    headers = {
        # Standard headers help avoid some trivial blocks; user-agent kept generic
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }

    batches = chunk_list(usernames, batch_size)

    results: Dict[str, int] = {}

    sem = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        async def worker(batch: List[str]) -> None:
            async with sem:
                mapping = await fetch_batch(session, batch, max_retries, request_timeout_sec)
                results.update(mapping)

        await asyncio.gather(*(worker(batch) for batch in batches))

    return results


def update_dataframe_with_counts(df: pd.DataFrame, counts: Dict[str, int], username_column: str) -> pd.DataFrame:
    # Vectorized mapping for speed on large CSVs
    normalized_usernames = (
        df[username_column]
        .astype(str)
        .map(normalize_username)
        .str.lower()
    )

    lookup = {k.lower(): v for k, v in counts.items()}
    mapped_counts = normalized_usernames.map(lookup)

    # Ensure followers_count column exists
    if "followers_count" not in df.columns:
        df["followers_count"] = 0

    # Where mapping found a value, use it; otherwise keep existing
    df.loc[mapped_counts.notna(), "followers_count"] = mapped_counts[mapped_counts.notna()].astype(int)
    return df


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Twitter followers_count for usernames without API (syndication endpoint).")
    parser.add_argument("input_csv", type=str, help="Path to input CSV containing a 'username' column.")
    parser.add_argument("--output-csv", type=str, default=None, help="Path to write updated CSV. Defaults to in-place overwrite.")
    parser.add_argument("--batch-size", type=int, default=100, help="Number of usernames per request batch (default: 100).")
    parser.add_argument("--concurrency", type=int, default=20, help="Number of concurrent requests (default: 20).")
    parser.add_argument("--max-retries", type=int, default=3, help="Maximum retries per batch on transient errors (default: 3).")
    parser.add_argument("--timeout-sec", type=float, default=15.0, help="Request timeout per batch (seconds, default: 15).")
    parser.add_argument("--username-column", type=str, default="username", help="Name of the username column in CSV (default: username).")
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    input_csv_path = args.input_csv
    output_csv_path = args.output_csv or input_csv_path

    # Load CSV
    try:
        df = pd.read_csv(input_csv_path, dtype={args.username_column: str}, keep_default_na=False)
    except Exception as exc:
        print(f"Failed to read CSV '{input_csv_path}': {exc}", file=sys.stderr)
        return 1

    if args.username_column not in df.columns:
        print(f"Input CSV does not have required column '{args.username_column}'. Columns found: {list(df.columns)}", file=sys.stderr)
        return 1

    # Prepare usernames (unique)
    usernames_series = df[args.username_column].astype(str).map(normalize_username)
    usernames_unique = sorted({u for u in usernames_series if u})

    if not usernames_unique:
        print("No usernames found to process.")
        return 0

    print(f"Fetching followers_count for {len(usernames_unique)} unique usernames...")

    counts: Dict[str, int] = asyncio.run(
        gather_followers_counts(
            usernames=usernames_unique,
            concurrency=max(1, args.concurrency),
            batch_size=max(1, args.batch_size),
            max_retries=max(0, args.max_retries),
            request_timeout_sec=max(5.0, float(args.timeout_sec)),
        )
    )

    # Update DataFrame
    df = df.copy()
    df = update_dataframe_with_counts(df, counts, args.username_column)

    # Write CSV back
    try:
        df.to_csv(output_csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
    except Exception as exc:
        print(f"Failed to write CSV '{output_csv_path}': {exc}", file=sys.stderr)
        return 1

    print(f"Wrote updated CSV to: {output_csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))