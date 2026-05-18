SHEET_ID = "1rAMV-_Xh2Q8wpgAJWzgzYbHu96UO9NmsD1xGHr2Xz1E"

RAW_REVIEWS_TAB = "Reviews (raw)"
SENTIMENT_CURRENT_TAB = "Sentiment - Current"
SENTIMENT_HISTORY_TAB = "Sentiment - History"
THEMES_LIBRARY_TAB = "Themes Library"
SENTIMENT_REVIEWS_TAB = "Sentiment - Reviews"
THEME_BREAKDOWN_TAB = "Theme Sentiment Breakdown"
DASHBOARD_TAB = "Dashboard"

LOCATION_HOTSPOT_MIN = 3  # min negative/mixed reviews on same theme at one location to surface on dashboard

GCP_PROJECT = "places-review-test-469517"
GCP_LOCATION = "us-central1"
GEMINI_MODEL = "gemini-2.5-flash-lite"

# 75 reviews per batch keeps the JSON response well under the 65k output token limit.
# March 2026 at 139 reviews hit the truncation threshold; 75 gives a comfortable margin.
BATCH_SIZE = 75

BASELINE_START = "2025-10-01"

APPROVED_THEMES = [
    "staff_friendliness",
    "staff_helpfulness",
    "equipment_reliability",
    "wash_quality",
    "vehicle_damage",
    "membership_subscription",
    "pricing",
    "service_recovery",
    "vacuums_amenities",
    "safety",
    "cleanliness",
    "facility_condition",
    "members_lounge",
    "general_positive",
    "general_negative",
]
