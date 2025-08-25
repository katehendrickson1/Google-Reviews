import os, sys, csv, re, math, json, pathlib, datetime, requests, gspread
from collections import Counter
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
# ---------- Config ----------
load_dotenv()  # loads .env in same folder

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")  # <-- put in .env
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL")  # optional, for Slack posting
STATE_FILE = "state_reviews.json"
SHEET_ID = "1rAMV-_Xh2Q8wpgAJWzgzYbHu96UO9NmsD1xGHr2Xz1E"

# List your locations here (Place ID + friendly name)
LOCATIONS = [
    {"place_id": "ChIJ-2-ZMugNU4cRXL9GFcRRrqM", "name": "Pleasant View"},
    {"place_id": "ChIJMY4HT7ABU4cRy4c9fdaxWvs", "name": "Layton"},
    {"place_id": "ChIJIUoOrwyNUocRUwzGQocvYUc", "name": "Kearns"},
    {"place_id": "ChIJs-ajHLKNUocRkkO6bkNDqvw", "name": "West Valley"},
    {"place_id": "ChIJZZ9Ay1qLUocRdx4fuIR4JO0", "name": "Murray"},
    {"place_id": "ChIJRRGDHveHUocRUs0phtO91cA", "name": "Draper"},
    {"place_id": "ChIJS-gO0piBTYcRQgGTvKmhWmw", "name": "Cedar Hills"},
    {"place_id": "ChIJBU3KaAF_TYcRDXRnINFqVrc", "name": "Lehi"},
    {"place_id": "ChIJr5Bc4XaXTYcRhiP8bvuO890", "name": "Provo"},
    {"place_id": "ChIJpVZeaADdyIkRKcYGPT7ToQ0", "name": "Mechanicsburg"},
    {"place_id": "ChIJuzMJq3vByIkR6G88vcr88B4", "name": "Lemoyne"},
    {"place_id": "ChIJG0ygTT-5yIkR6mwGt6AHOLA", "name": "Lower Paxton"},
    {"place_id": "ChIJbYGn7RvHyIkRdwfpTJLHpmY", "name": "Linglestown"},
    {"place_id": "ChIJb82CdhoDxokRkN84hrDFo8M", "name": "Lebanon"},
    {"place_id": "ChIJlzkojDM_z4kREheIVuR8Ke8", "name": "Selinsgrove"},
    {"place_id": "ChIJq3MX6zWNyIkR2PJtX4UD1dg", "name": "West Manchester"},
    {"place_id": "ChIJoXCCLyYtyIkRpTKJEcL335U", "name": "Mt. Airy"},
    {"place_id": "ChIJdaPibmq9t4kR8nlIXd3l-pQ", "name": "Clinton"},
    {"place_id": "ChIJMzZkas_9x4kRFjoJslJnhYk", "name": "Middle River"}
    # Add more: {"place_id": "...", "name": "..."},
]

# How many newest reviews to show in Slack & reports
N_NEWEST = 5

# ---------- Helpers ----------
def stars_to_sentiment(stars: float) -> float:
    # map 1..5 stars to -1..1
    return max(-1.0, min(1.0, (stars - 3.0) / 2.0))

POS_WORDS = set("""
amazing awesome great excellent friendly clean quick fast helpful convenient best love loved efficient thorough shiny membership value
""".split())
NEG_WORDS = set("""
bad rude slow dirty expensive broken confusing hard worse worst terrible awful disappointed streaks damage queue wait waiting scratch
""".split())

def tokenize(text: str):
    return re.findall(r"[a-zA-Z']+", (text or "").lower())

def label_from_score(s):
    if s >= 0.25: return "Positive"
    if s <= -0.25: return "Negative"
    return "Mixed/Neutral"

def iso_utc_from_unix(ts):
    try:
        return datetime.datetime.utcfromtimestamp(int(ts)).isoformat() + "Z"
    except Exception:
        return None
    
import json, datetime, os

#--- helper to get count of reviews weekly ---#
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)

def parse_iso_z(s):
    # "2025-08-16T00:45:46Z" -> aware datetime in UTC
    try:
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1]
        return datetime.datetime.fromisoformat(s).replace(tzinfo=datetime.timezone.utc)
    except Exception:
        return None

#-- 7-day filter (get reviews only from last week) --#
def reviews_since(reviews, since_dt_utc):
    out = []
    for r in reviews:
        dt = parse_iso_z(r.get("publishTime"))
        if dt and dt >= since_dt_utc:
            out.append(r)
    return out


#--- google sheets upload helper ---#
def upload_to_google_sheets(csv_path, worksheet_name="Google Reviews Data"):
    # Define the scope
    scope = ["https://www.googleapis.com/auth/spreadsheets"]

    # Load service account credentials
    creds = Credentials.from_service_account_file("service_account.json", scopes=scope)
    client = gspread.authorize(creds)

    # Open the target sheet
    sh = client.open_by_key(SHEET_ID)

    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=2000, cols=20)

    # Read the CSV and push to sheet
    import csv
    with open(csv_path, "r", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    
    ws.clear()
    ws.update(rows, "A1")
    print(f"✅ Uploaded {csv_path} to Google Sheet (ID {SHEET_ID}) on tab '{worksheet_name}'")



# ---------- API calls ----------
def fetch_new_api(place_id):
    place_id = place_id.strip()
    url = f"https://places.googleapis.com/v1/places/{place_id}"
    headers = {
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": "id,displayName,rating,userRatingCount"
    }
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        print("NEW API ERROR:", r.status_code, r.text)
        r.raise_for_status()
    return r.json()


def fetch_legacy_newest(place_id, language="en"):
    base = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "rating,user_ratings_total,reviews,geometry,url",
        "reviews_sort": "newest",
        "language": language,
        "key": API_KEY
    }
    r = requests.get(base, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return (data.get("result") or {})

# ---------- Sentiment & theming ----------
def summarize_sentiment(avg_rating, reviews_text_and_star):
    # star-based sentiment from review stars OR fallback to avg rating
    star_scores = []
    pos_hits = neg_hits = 0
    tokens_all = []

    for stars, text in reviews_text_and_star:
        if isinstance(stars, (int, float)):
            star_scores.append(stars_to_sentiment(stars))
        t = tokenize(text or "")
        tokens_all.extend(t)
        pos_hits += sum(1 for w in t if w in POS_WORDS)
        neg_hits += sum(1 for w in t if w in NEG_WORDS)

    if star_scores:
        star_sent = sum(star_scores) / len(star_scores)
    else:
        star_sent = stars_to_sentiment(avg_rating) if avg_rating else 0.0

    text_sent = 0.0
    if pos_hits or neg_hits:
        text_sent = (pos_hits - neg_hits) / max(1, (pos_hits + neg_hits))
        text_sent = max(-1.0, min(1.0, text_sent))

    overall = 0.7 * star_sent + 0.3 * text_sent
    label = label_from_score(overall)

    # themes
    stop = set("""
a an the and or of to in for on at with from by is are was were be been it this that those these very really just quite not
we i you they he she them us our my your their
""".split())
    tokens = [w for w in tokens_all if len(w) > 2 and w not in stop]
    common = Counter(tokens).most_common(30)
    likes = [w for w, _ in common if w in POS_WORDS][:6]
    cons  = [w for w, _ in common if w in NEG_WORDS][:6]

    return {"score": round(overall, 3), "label": label, "likes": likes, "cons": cons}

# ---------- Report generation ----------
def ensure_dir(path):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def write_markdown_report(folder, loc_name, maps_url, rating, count, review_summary, newest_reviews, sentiment):
    md = []
    md.append(f"# {loc_name}")
    md.append("")
    md.append(f"- **Google Rating:** {rating}  ({count} total)")
    if maps_url:
        md.append(f"- **Google Maps:** {maps_url}")
    md.append(f"- **Automated sentiment:** **{sentiment['label']}** (score {sentiment['score']})")
    if review_summary:
        # new API's summary is structured; show its 'overview' if present, otherwise dump JSON
        overview = review_summary.get("overview")
        if overview:
            md.append("")
            md.append("## Google Review Data")
            md.append(overview.strip())
    md.append("")
    md.append("## Newest Reviews")
    if not newest_reviews:
        md.append("_None returned by API_")
    else:
        for r in newest_reviews[:N_NEWEST]:
            stars = r.get("rating")
            text = r.get("text") or ""
            when = r.get("publishTime") or r.get("relativeTime") or ""
            clean_text = text.strip().replace("\n", " ")
            md.append(f"- **[{stars}★] {when}** — {clean_text[:400]}{'…' if len(clean_text) > 400 else ''}")


    path = os.path.join(folder, f"{loc_name.replace(' ', '_')}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    return path
def post_to_slack(webhook_url, loc_name, maps_url, rating, count, sentiment, newest_reviews,weekly_new=None, sample_7d=None):
    if not webhook_url:
        return

    lines = []
    lines.append(f"*📍 {loc_name}*")
    lines.append(f"⭐ {rating} ({count} reviews)")
    if weekly_new is None:
        lines.append("🆕 New this week: — (first run)")
    else:
        lines.append(f"🆕 New this week: *{weekly_new}*")
    lines.append(f"🙂 Sentiment: *{sentiment['label']}* ({sentiment['score']})")
    if maps_url:
        lines.append(f"<{maps_url}|Open in Google Maps>")

    if newest_reviews:
        lines.append("\n*🆕 Newest reviews:*")
        for r in newest_reviews[:N_NEWEST]:
            stars = r.get("rating")
            # Prefer human-friendly relative time
            when = r.get("relativeTime") or ""
            text = (r.get("text") or "").strip().replace("\n", " ")
            if len(text) > 240:
                text = text[:240] + "…"
            lines.append(f"• [{stars}★] {when} — {text}")

    payload = {"text": "\n".join(lines)}
    try:
        requests.post(webhook_url, json=payload, timeout=15).raise_for_status()
    except Exception as e:
        print("Slack post failed:", e)

def main():
    today = datetime.date.today().isoformat()
    out_dir = os.path.join("reports", today)
    state = load_state()
    ensure_dir(out_dir)

    summary_rows = []
    for loc in LOCATIONS:
        pid = loc["place_id"]
        name = loc.get("name") or pid

        # --- New API ---
        new = fetch_new_api(pid)

        # Prefer your custom location name for output & filenames
        loc_name = loc.get("name") or (new.get("displayName") or {}).get("text") or pid

        maps_url = new.get("googleMapsUri")
        avg_rating = new.get("rating")
        count = new.get("userRatingCount", 0)  # <-- New API field name
        review_summary = new.get("reviewSummary")
        new_reviews = new.get("reviews") or []

        # --- Legacy API for newest reviews ---
        legacy = fetch_legacy_newest(pid, language="en")
        legacy_reviews = legacy.get("reviews", []) or []
        normalized_newest = []
        for r in legacy_reviews:
            normalized_newest.append({
                "author": r.get("author_name"),
                "rating": r.get("rating"),
                "text": r.get("text"),
                "relativeTime": r.get("relative_time_description"),
                "publishTime": iso_utc_from_unix(r.get("time")),
                "profilePhotoUrl": r.get("profile_photo_url"),
            })

            # --- 7-day filtered newest reviews ---
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        seven_days_ago = now_utc - datetime.timedelta(days=7)
        newest_week = reviews_since(normalized_newest, seven_days_ago)
        sample_7d = len(newest_week)  # keep this metric if you like

        # --- Weekly delta from total count ---
        prev = (state.get(pid) or {}).get("userRatingCount")
        if isinstance(prev, (int, float)):
            weekly_new = max(0, count - int(prev))
        else:
            # First baseline: approximate “new this week” by actual 7-day reviews
            weekly_new = len(newest_week)
            weekly_new_clamped = weekly_new



        # Record the latest count for next week
        state[pid] = {
            "userRatingCount": int(count) if count is not None else None,
            "lastRun": datetime.date.today().isoformat(),
        }
        # --- Sentiment ---
        # --- Sentiment (same as you have) ---
        pairs = []
        for r in normalized_newest[:5]:
            pairs.append((r.get("rating"), r.get("text")))
        if not pairs:
            for r in new_reviews[:5]:
                stars = r.get("rating")
                txt = (r.get("originalText") or {}).get("text") or r.get("text")
                pairs.append((stars, txt))
        sentiment = summarize_sentiment(avg_rating, pairs)

        # --- Terminal output per location ---
        print("\n===============================")
        print(f"📍 {loc_name}")
        print(f"⭐ Avg rating: {avg_rating} ({count} reviews)")
        if weekly_new_clamped is None:
            print("🆕 New this week: — (first run baseline)")
        else:
            print(f"🆕 New this week: {weekly_new_clamped}")
        print(f"🙂 Sentiment: {sentiment['label']} ({sentiment['score']})")
        if newest_week:
            print("Newest reviews:")
            for r in newest_week[:N_NEWEST]:
                author = r.get("author") or "Anonymous"
                stars = r.get("rating")
                text = (r.get("text") or "").strip().replace("\n", " ")
                if len(text) > 140: text = text[:140] + "…"
                print(f" - {author} ({stars}★): {text}")
        else:
            print("No reviews in the last 7 days.")

        if SLACK_WEBHOOK:
            post_to_slack(
                SLACK_WEBHOOK, loc_name, maps_url, avg_rating, count, sentiment,
                newest_week,  # <-- only the filtered list
                weekly_new=weekly_new_clamped,
                sample_7d=sample_7d
            )
        else:
            print("⚠️ SLACK_WEBHOOK_URL not set; skipping Slack")

        # --- Optional: still write Markdown + CSV for archiving ---
        md_path = write_markdown_report(
            out_dir, loc_name, maps_url, avg_rating, count, review_summary,
            newest_week,  # <-- only the filtered list
            sentiment
        )

        summary_rows.append({
            "date": today,
            "place": name,
            "place_id": pid,
            "rating": avg_rating,
            "review_count": count,
            "new_reviews_week": weekly_new_clamped if weekly_new_clamped is not None else "",
            "sentiment_label": sentiment["label"],
            "sentiment_score": sentiment["score"],
            "report_path": md_path.replace("\\", "/"),
            "maps_url": maps_url or ""
        })
        
    save_state(state)

    # Write CSV summary
    if summary_rows:
        csv_path = os.path.join(out_dir, "summary.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
            writer.writeheader()
            writer.writerows(summary_rows)
        print(f"\n✅ Saved CSV + Markdown reports in: {out_dir}")
    
    if summary_rows:
        csv_path = os.path.join(out_dir, "summary.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
            writer.writeheader()
            writer.writerows(summary_rows)
        print(f"\n✅ Saved CSV + Markdown reports in: {out_dir}")

    # Upload to Google Sheets
        upload_to_google_sheets(csv_path, worksheet_name="Google Reviews Data")

if __name__ == "__main__":
    main()
