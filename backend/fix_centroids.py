"""Reset centroid/jittered PDVs back to pending so OpenAI can geocode them properly."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]


def main():
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # These PDVs were geocoded to a centroid and then jittered — they need
    # a real geocode via OpenAI. We identify them: L1/L2 found the real address,
    # L3 used the localidad centroid. The jittered ones have coords that don't
    # match the original centroid anymore, but we can find ALL that had >3 at
    # the same rounded coordinates before jitter. Easier: just reset any PDV
    # whose geocoding_attempts >= 2 (meaning L1 and L2 both failed → was L3).
    #
    # Actually, the simplest: reset geocoding_attempts >= 2 to pending.
    # These are the ones where Nominatim failed on the exact address.

    res = sb.table("pdv").select("id", count="exact").eq("geocoding_status", "ok").gte("geocoding_attempts", 2).execute()
    print(f"PDVs geocoded by centroid fallback (attempts>=2): {res.count}")

    if res.count and res.count > 0:
        # Reset them to pending with lat/lng cleared
        updated = sb.table("pdv").update({
            "geocoding_status": "pending",
            "geocoding_attempts": 0,
            "lat": None,
            "lng": None,
        }).eq("geocoding_status", "ok").gte("geocoding_attempts", 2).execute()
        print(f"Reset {len(updated.data)} PDVs to pending for OpenAI re-geocoding")
    else:
        print("No centroid PDVs found to reset")

    # Also reset any failed ones
    failed = sb.table("pdv").update({
        "geocoding_status": "pending",
        "geocoding_attempts": 0,
        "lat": None,
        "lng": None,
    }).eq("geocoding_status", "failed").execute()
    print(f"Reset {len(failed.data)} failed PDVs to pending too")


if __name__ == "__main__":
    main()
