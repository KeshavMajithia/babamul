#!/usr/bin/env python3
"""Example: Using the BOOM filter API via babamul.

This script demonstrates how to:
1. Authenticate with the BOOM API
2. Explore the available filter schema
3. Build and test a filter pipeline
4. Save the filter for production use

Prerequisites:
    export BOOM_API_BASE_URL="https://boom-api.nrp-nautilus.io/api"

    Or pass the URL directly to login().
"""

import json
import os

import babamul


def main() -> None:
    # ---------------------------------------------------------------
    # 1. Login to get a JWT token
    # ---------------------------------------------------------------
    # You can either:
    #   a) Set BOOM_API_TOKEN env var with a pre-existing JWT, or
    #   b) Login with username/password to get one
    boom_url = os.environ.get(
        "BOOM_API_BASE_URL", "https://boom-api.nrp-nautilus.io/api"
    )
    username = os.environ.get("BOOM_USERNAME", "admin")
    password = os.environ.get("BOOM_PASSWORD", "")

    if not password:
        print("Set BOOM_PASSWORD env var or BOOM_API_TOKEN to skip login.")
        return

    print(f"Logging in to {boom_url} as '{username}'...")
    token = babamul.login(username, password, base_url=boom_url)
    print(f"Got token: {token[:20]}...")

    # ---------------------------------------------------------------
    # 2. Explore the filter schema
    # ---------------------------------------------------------------
    print("\n--- ZTF Filter Schema (top-level fields) ---")
    schema = babamul.get_filter_schema("ZTF", token=token)
    if "fields" in schema:
        for field in schema["fields"]:
            name = field.get("name", "?")
            ftype = field.get("type", "?")
            if isinstance(ftype, dict):
                ftype = ftype.get("type", ftype)
            print(f"  {name}: {ftype}")
    else:
        print(json.dumps(schema, indent=2)[:500])

    # ---------------------------------------------------------------
    # 3. Build a simple filter pipeline
    # ---------------------------------------------------------------
    # This filter selects ZTF alerts where:
    #   - The real/bogus score (drb) is > 0.5 (likely real)
    #   - The PSF magnitude is brighter than 19
    #
    # NOTE: BOOM requires the last stage to be a $project that
    # includes objectId: 1 (the API validates this).
    pipeline = [
        {
            "$match": {
                "candidate.drb": {"$gt": 0.5},
                "candidate.magpsf": {"$lt": 19.0},
            }
        },
        {
            "$project": {
                "_id": 1,
                "objectId": 1,
                "candidate.ra": 1,
                "candidate.dec": 1,
                "candidate.magpsf": 1,
                "candidate.drb": 1,
                "candidate.jd": 1,
            }
        },
    ]

    # ZTF public data uses programid=1
    permissions = {"ZTF": [1]}

    # ---------------------------------------------------------------
    # 4. Test the filter (count first, then fetch a few results)
    # ---------------------------------------------------------------
    # Use a 2-day JD window covering April 8-9, 2026
    # April 8, 2026 ~= JD 2461404.5
    start_jd = 2461404.5
    end_jd = 2461406.5

    print(f"\n--- Testing filter (JD {start_jd} to {end_jd}) ---")

    count_result = babamul.test_filter_count(
        pipeline=pipeline,
        survey="ZTF",
        permissions=permissions,
        start_jd=start_jd,
        end_jd=end_jd,
        token=token,
    )
    print(f"Matching alerts: {count_result.count}")

    if count_result.count > 0:
        test_result = babamul.test_filter(
            pipeline=pipeline,
            survey="ZTF",
            permissions=permissions,
            start_jd=start_jd,
            end_jd=end_jd,
            limit=5,
            sort_by="candidate.magpsf",
            sort_order="ascending",
            token=token,
        )
        print(f"\nTop {len(test_result.results)} brightest matches:")
        for i, alert in enumerate(test_result.results):
            obj_id = alert.get("objectId", "?")
            mag = alert.get("candidate", {}).get("magpsf", "?")
            drb = alert.get("candidate", {}).get("drb", "?")
            print(f"  {i+1}. {obj_id}  mag={mag}  drb={drb}")

    # ---------------------------------------------------------------
    # 5. Save the filter
    # ---------------------------------------------------------------
    print("\n--- Saving filter ---")
    new_filter = babamul.create_filter(
        name="Bright Real Transients",
        pipeline=pipeline,
        survey="ZTF",
        permissions=permissions,
        description="ZTF alerts with drb > 0.5 and mag < 19",
        token=token,
    )
    print(f"Created filter: {new_filter.name} (id={new_filter.id})")

    # ---------------------------------------------------------------
    # 6. List all filters
    # ---------------------------------------------------------------
    print("\n--- All filters ---")
    filters = babamul.get_filters(token=token)
    for f in filters:
        status = "active" if f.active else "inactive"
        print(f"  [{status}] {f.name} ({f.survey}) - {len(f.fv)} version(s)")


if __name__ == "__main__":
    main()
