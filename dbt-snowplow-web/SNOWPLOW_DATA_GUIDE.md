# Guide to Generating Relevant Snowplow Data

## Overview

This guide explains how to generate realistic, relevant Snowplow event data for testing and benchmarking with the dbt-snowplow-web package.

## Key Requirements for Relevant Snowplow Data

### 1. **Event Relationships & Consistency**

Events must have proper relationships:

- **Page View Events**: Each page view should have a unique `page_view_id` in the `web_page_context`
- **Web Vitals Events**: Must share the same `page_view_id` as the related `page_view` event
- **Page Ping Events**: Must share the same `page_view_id` and `domain_sessionid` as the related `page_view`
- **Session Consistency**: All events in the same session must share:
  - `domain_sessionid`
  - `domain_userid`
  - `network_userid` (if available)

### 2. **Required Fields**

Critical fields that must be present:

```python
# Required timestamps
collector_tstamp     # NOT NULL - when event was received by collector
derived_tstamp       # Calculated timestamp, often = collector_tstamp
dvce_created_tstamp  # When event was created on device
etl_tstamp          # When event was processed by ETL
dvce_sent_tstamp    # When event was sent from device

# Required identifiers
event_id            # Unique UUID for each event
domain_sessionid    # Session identifier
domain_userid       # User identifier (persistent across sessions)

# Event metadata
event               # Event type: 'page_view', 'page_ping', 'unstruct', etc.
event_name          # For unstructured events: 'web_vitals', etc.
platform            # Usually 'web'
app_id              # Application identifier

# Contexts (JSON strings)
contexts_com_snowplowanalytics_snowplow_web_page_1  # Must contain page_view_id
unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1  # Web vitals data
```

### 3. **Event Type Correctness**

- **`page_view` events**: `event='page_view'`, `event_name='page_view'`
- **`page_ping` events**: `event='page_ping'`, `event_name='page_ping'`
- **`web_vitals` events**: `event='unstruct'`, `event_name='web_vitals'`
- **Web vitals data**: Must be in `unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1` field

### 4. **Timing Relationships**

Events should have realistic timing:

- **Page View**: Base timestamp
- **Web Vitals**: 100-2000ms after page_view (measures page load performance)
- **Page Pings**: 10 seconds apart, starting after page_view (engagement tracking)

### 5. **Context Structure**

Contexts must be valid JSON arrays:

```python
# Web Page Context (REQUIRED for page_view, web_vitals, page_ping)
web_page_context = [{'id': 'unique-page-view-uuid'}]

# Web Vitals (REQUIRED for web_vitals events)
web_vitals = [{
    'cls': 0.05,                    # Cumulative Layout Shift (0-1)
    'fcp': 1500,                    # First Contentful Paint (ms)
    'fid': 50,                       # First Input Delay (ms)
    'inp': 75,                       # Interaction to Next Paint (ms)
    'lcp': 2500,                     # Largest Contentful Paint (ms)
    'navigation_type': 'navigate',   # Navigation type
    'ttfb': 200                      # Time to First Byte (ms)
}]

# UA Parser Context
ua_context = [{
    'deviceFamily': 'iPhone',
    'osFamily': 'iOS',
    'useragentFamily': 'Safari'
}]

# IAB Context (for bot detection)
iab_context = [{
    'category': 'BROWSER',
    'spiderOrRobot': False
}]

# YAUAA Context
yauaa_context = [{
    'agentClass': 'Browser',
    'deviceClass': 'Phone'
}]
```

## Best Practices

### 1. **Use Realistic Data Patterns**

- **Geographic Distribution**: Use real countries/cities with proper coordinates
- **User Agents**: Use actual browser/device user agent strings
- **Page URLs**: Use realistic URL structures
- **Referrer Diversity**: Include various referrer sources (search, direct, social)

### 2. **Session Modeling**

- Generate realistic session lengths (1-30 minutes)
- Multiple page views per session (1-10 pages)
- Proper session start/end timestamps
- Consistent user behavior within sessions

### 3. **Event Volume**

- **Page Views**: Base event type
- **Web Vitals**: 1 per page_view (optional but recommended)
- **Page Pings**: 0-12 per page_view (depends on engagement time)

### 4. **Data Quality**

- **No NULL collector_tstamp**: Required field
- **Valid JSON**: All context fields must be valid JSON
- **UUID Format**: All IDs should be valid UUIDs
- **Timestamp Consistency**: Timestamps should be in chronological order

## Current Implementation

The `gen_events.py` script already implements:

✅ Proper event relationships (page_view_id shared across events)  
✅ Correct event types (`unstruct` for web_vitals)  
✅ Realistic timing (web_vitals after page_view, page_pings at intervals)  
✅ Valid context structures (JSON arrays)  
✅ Session consistency (shared identifiers)  
✅ Mobile/desktop distribution  
✅ Geographic diversity  

## Sample Data Sources

### Official Snowplow Sample Data

```bash
# Download official sample dataset
curl -o Web_Analytics_sample_events.csv \
  https://snowplow-demo-datasets.s3.eu-central-1.amazonaws.com/Web_Analytics/Web_Analytics_sample_events.csv
```

Note: This data may need JSON formatting fixes for proper parsing.

### Generating with gen_events.py

```bash
# Generate by row count
python gen_events.py --rows 10000

# Generate by size (recommended for benchmarks)
python gen_events.py --gb 1
```

## Validation Checklist

Before using generated data, verify:

- [ ] All events have `collector_tstamp` (NOT NULL)
- [ ] All page_view events have `web_page_context` with `id`
- [ ] Web vitals events have `event='unstruct'` and `event_name='web_vitals'`
- [ ] Web vitals events share `page_view_id` with related page_view
- [ ] Page ping events share `domain_sessionid` with page_view
- [ ] Timestamps are in chronological order
- [ ] All JSON contexts are valid JSON
- [ ] Session identifiers are consistent within sessions

## Common Issues

### Issue: Events not appearing in dbt models

**Causes:**
- Missing `page_view_id` in web_page_context
- Wrong `event` type (should be `unstruct` for web_vitals, not `struct`)
- Invalid JSON in context fields
- Missing `collector_tstamp`

### Issue: Reduced row counts

**Causes:**
- Events filtered out by bot detection (check `iab_context.spiderOrRobot`)
- Session timing issues (events outside session window)
- Missing required contexts

## References

- [Snowplow Web Package Documentation](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-models/dbt-web-data-model/)
- [dbt-snowplow-web Package](https://github.com/snowplow/dbt-snowplow-web)
- [Snowplow Event Structure](https://docs.snowplow.io/docs/understanding-your-pipeline/canonical-event/)

