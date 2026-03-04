import time
import json
import requests
import pandas as pd
from datetime import datetime

API_URL = "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery?api-version=3.0-preview.1"

VSCODE_TARGET = "Microsoft.VisualStudio.Code"

# All official VSCode marketplace categories
CATEGORIES = [
    "AI",
    "Azure",
    "Chat",
    "Data Science",
    "Debuggers",
    "Education",
    "Extension Packs",
    "Formatters",
    "Keymaps",
    "Language Packs",
    "Linters",
    "Notebooks",
    "Other",
    "Programming Languages",
    "SCM Providers",
    "Snippets",
    "Testing",
    "Themes",
    "Visualization",
]

HEADERS = {
    "Accept": "application/json;api-version=3.0-preview.1",
    "Content-Type": "application/json",
}

FLAGS = 914
PAGE_SIZE = 200
SLEEP_EVERY_N_PAGES = 5   # pause every N pages within a category
SLEEP_SECONDS = 1.5       # seconds to sleep


def stat_value(stats, name):
    if not isinstance(stats, list):
        return None
    for s in stats:
        if s.get("statisticName") == name:
            return s.get("value")
    return None


def marketplace_url(publisher, extension_name):
    return f"https://marketplace.visualstudio.com/items?itemName={publisher}.{extension_name}"


def post_with_retry(session, payload, retries=6):
    backoff = 2
    last = None
    for _ in range(retries):
        r = session.post(API_URL, headers=HEADERS, data=json.dumps(payload), timeout=90)
        last = r
        if r.status_code in (429, 500, 502, 503, 504):
            print(f" [rate-limit/error {r.status_code}, waiting {backoff}s]", end="", flush=True)
            time.sleep(backoff)
            backoff *= 2
            continue
        if r.status_code == 400:
            # API hard page limit reached for this category/query
            return None
        r.raise_for_status()
        return r.json()
    if last is not None:
        last.raise_for_status()
    raise RuntimeError("Request failed with no response")


def query_by_category(session, category, page_number):
    payload = {
        "filters": [{
            "pageNumber": page_number,
            "pageSize": PAGE_SIZE,
            "criteria": [
                {"filterType": 8, "value": VSCODE_TARGET},   # Target: VSCode
                {"filterType": 5, "value": category},         # Category
            ],
            "sortBy": 4,    # InstallCount desc
            "sortOrder": 0
        }],
        "assetTypes": [],
        "flags": FLAGS
    }
    return post_with_retry(session, payload)


def extract_rows(category, extensions):
    rows = []
    for e in extensions:
        publisher = (e.get("publisher") or {}).get("publisherName")
        ext_name = e.get("extensionName")
        display = e.get("displayName")
        versions = e.get("versions") or []
        latest_version = versions[0].get("version") if versions else None

        stats = e.get("statistics") or []
        installs = stat_value(stats, "install")
        avg_rating = stat_value(stats, "averagerating")
        rating_count = stat_value(stats, "ratingcount")

        rows.append({
            "MatchType": "Category",
            "MatchValue": category,
            "IDE": "VSCode",
            "Target": VSCODE_TARGET,
            "Publisher": publisher,
            "ExtensionId": ext_name,
            "DisplayName": display,
            "LatestVersion": latest_version,
            "InstallCount": installs,
            "AverageRating": avg_rating,
            "RatingCount": rating_count,
            "MarketplaceTags": ",".join(e.get("tags") or []),
            "MarketplaceUrl": marketplace_url(publisher, ext_name) if publisher and ext_name else None,
        })
    return rows


def fetch_category(session, category):
    cat_rows = []
    page = 1
    while True:
        print(f"    Page {page}...", end=" ", flush=True)

        data = query_by_category(session, category, page)

        # 400 = API page limit hit for this category
        if data is None:
            print(f"API limit reached at page {page}, stopping category.")
            break

        exts = (data.get("results") or [{}])[0].get("extensions") or []
        if not exts:
            print("no more results.")
            break

        cat_rows.extend(extract_rows(category, exts))
        print(f"{len(exts)} extensions (category total: {len(cat_rows)})")

        if len(exts) < PAGE_SIZE:
            break

        # Polite sleep every N pages to avoid hammering the API
        if page % SLEEP_EVERY_N_PAGES == 0:
            time.sleep(SLEEP_SECONDS)

        page += 1

    return cat_rows


def main():
    all_rows = []

    print("Starting VSCode Marketplace download by category...")
    print(f"Categories to fetch: {len(CATEGORIES)}\n")

    with requests.Session() as session:
        for i, category in enumerate(CATEGORIES, 1):
            print(f"[{i}/{len(CATEGORIES)}] Category: '{category}'")
            cat_rows = fetch_category(session, category)
            all_rows.extend(cat_rows)
            print(f"  => {len(cat_rows)} extensions fetched. Grand total so far: {len(all_rows)}\n")
            # Brief pause between categories
            time.sleep(SLEEP_SECONDS)

    df = pd.DataFrame(all_rows)
    before_dedup = len(df)
    df.drop_duplicates(subset=["Publisher", "ExtensionId"], inplace=True)
    df.sort_values(by=["Publisher", "ExtensionId"], inplace=True)

    today = datetime.now().strftime("%Y%m%d")
    out_csv = f"vscode_marketplace_{today}.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    print(f"[OK] CSV generated: {out_csv}")
    print(f"[OK] Raw rows collected: {before_dedup}")
    print(f"[OK] Unique extensions after dedup: {len(df)}")


if __name__ == "__main__":
    main()