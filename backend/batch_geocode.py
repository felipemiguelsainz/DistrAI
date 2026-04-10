"""Batch geocode: same logic as the web UI button, runs all pending."""
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from supabase import create_client
from services.geocoding import geocode_pending

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]


async def main():
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Fix corrupted localidad if still there
    sb.table("pdv").update({"localidad": "VILLA ESPAÑA"}).eq("localidad", "VILLA ESPA#A").execute()

    # Count pending
    pending = sb.table("pdv").select("id", count="exact").eq("geocoding_status", "pending").execute()
    total_pending = pending.count or 0
    print(f"Pending: {total_pending}")

    if total_pending == 0:
        print("Nothing to do.")
        return

    # Loop in batches of 1000 (PostgREST server-side max) until no pending left
    jobs: dict = {}
    job_id = "batch"
    round_num = 0
    total_processed = 0

    while total_pending > 0:
        round_num += 1
        batch_size = min(total_pending, 1000)
        jobs[job_id] = {"total": 0, "processed": 0, "errors": 0, "status": "running"}
        print(f"Round {round_num}: {total_pending} pending, processing up to {batch_size}...")

        result = await geocode_pending(sb, batch_size, jobs, job_id)
        total_processed += result.get("processed", 0)
        print(f"  Round {round_num} done: {result}")

        # Re-check pending count
        pending_res = sb.table("pdv").select("id", count="exact").eq("geocoding_status", "pending").execute()
        total_pending = pending_res.count or 0
        print(f"  Remaining pending: {total_pending}")

    print(f"\nAll done. Total processed: {total_processed}")


if __name__ == "__main__":
    t0 = time.time()
    asyncio.run(main())
    print(f"\nDone in {(time.time() - t0)/60:.1f} minutes")
