#!/usr/bin/env python3
"""
Script to generate events.csv files with Snowplow event data for yesterday and today.
Can generate based on target GB size instead of row count.
"""

import csv
import uuid
import random
from datetime import datetime, timedelta
import json
import os
import sys

def generate_event_data(target_date, num_events=1000):
    """Generate sample Snowplow event data for a specific date."""
    
    # Sample data for variety
    countries = ['US', 'CA', 'GB', 'DE', 'FR', 'JP', 'AU', 'BR', 'IN', 'MX']
    cities = ['New York', 'London', 'Berlin', 'Paris', 'Tokyo', 'Sydney', 'São Paulo', 'Mumbai', 'Mexico City']
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36'
    ]
    
    pages = [
        'https://example.com/home',
        'https://example.com/products',
        'https://example.com/about',
        'https://example.com/contact',
        'https://example.com/blog'
    ]
    
    # Event names to randomly select from
    event_names = ['page_ping', 'web_vitals', 'cmp_visible', 'consent_preferences', 'unstruct', 'struct', 'page_view']
    
    events = []
    
    for i in range(num_events):
        # Generate timestamps for the target date
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        base_time = datetime.combine(target_date, datetime.min.time().replace(hour=hour, minute=minute, second=second))
        # Add milliseconds for compatibility with dbt models
        base_time = base_time.replace(microsecond=random.randint(0, 999999))
        collector_tstamp = base_time
        dvce_created_tstamp = base_time - timedelta(seconds=random.randint(1, 5), microseconds=random.randint(0, 999999))
        etl_tstamp = base_time + timedelta(seconds=random.randint(1, 3), microseconds=random.randint(0, 999999))
        
        # Generate event data
        event_id = str(uuid.uuid4())
        user_id = str(uuid.uuid4())
        domain_userid = str(uuid.uuid4())
        domain_sessionid = str(uuid.uuid4())
        network_userid = str(uuid.uuid4())
        
        country = random.choice(countries)
        city = random.choice(cities)
        user_agent = random.choice(user_agents)
        page_url = random.choice(pages)
        
        # Randomly select event name
        event_name = random.choice(event_names)
        
        # Generate contexts (simplified JSON)
        ua_context = [{
            'deviceFamily': 'iPhone' if 'iPhone' in user_agent else 'Desktop',
            'osFamily': 'iOS' if 'iPhone' in user_agent else 'Windows',
            'useragentFamily': 'Safari' if 'Safari' in user_agent else 'Chrome'
        }]
        
        web_page_context = [{'id': str(uuid.uuid4())}]
        iab_context = [{'category': 'BROWSER', 'spiderOrRobot': False}]
        yauaa_context = [{'agentClass': 'Browser', 'deviceClass': 'Phone' if 'Mobile' in user_agent else 'Desktop'}]
        
        # Generate web vitals
        web_vitals = [{
            'cls': round(random.uniform(0.01, 0.1), 3),
            'fcp': random.randint(100, 500),
            'fid': random.randint(10, 100),
            'inp': random.randint(10, 100),
            'lcp': random.randint(1000, 3000),
            'navigation_type': 'navigate',
            'ttfb': random.randint(50, 300)
        }]
        
        event = [
            'default',  # app_id
            'web',      # platform
            etl_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # etl_tstamp with milliseconds
            collector_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # collector_tstamp with milliseconds
            dvce_created_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # dvce_created_tstamp with milliseconds
            'page_view',  # event
            event_id,  # event_id
            '',  # txn_id
            'eng.gcp-dev1',  # name_tracker
            'js-2.17.2',  # v_tracker
            'ssc-2.1.2-googlepubsub',  # v_collector
            'beam-enrich-1.4.2-rc1-common-1.4.2-rc1',  # v_etl
            user_id,  # user_id
            '',  # user_ipaddress
            str(uuid.uuid4()),  # user_fingerprint
            domain_userid,  # domain_userid
            '1',  # domain_sessionidx
            network_userid,  # network_userid
            country,  # geo_country
            '',  # geo_region
            city,  # geo_city
            '',  # geo_zipcode
            str(random.uniform(-90, 90)),  # geo_latitude
            str(random.uniform(-180, 180)),  # geo_longitude
            '',  # geo_region_name
            '',  # ip_isp
            '',  # ip_organization
            '',  # ip_domain
            '',  # ip_netspeed
            page_url,  # page_url
            'Sample Page',  # page_title
            'https://www.google.com/',  # page_referrer
            'https',  # page_urlscheme
            'example.com',  # page_urlhost
            '443',  # page_urlport
            '/',  # page_urlpath
            '',  # page_urlquery
            '',  # page_urlfragment
            'https',  # refr_urlscheme
            'www.google.com',  # refr_urlhost
            '443',  # refr_urlport
            '/',  # refr_urlpath
            '',  # refr_urlquery
            '',  # refr_urllfragment
            'search',  # refr_medium
            'Google',  # refr_source
            '',  # refr_term
            '',  # mkt_medium
            '',  # mkt_source
            '',  # mkt_term
            '',  # mkt_content
            '',  # mkt_campaign
            '',  # se_category
            '',  # se_action
            '',  # se_label
            '',  # se_property
            '',  # se_value
            '',  # tr_orderid
            '',  # tr_affiliation
            '',  # tr_total
            '',  # tr_tax
            '',  # tr_shipping
            '',  # tr_city
            '',  # tr_state
            '',  # tr_country
            '',  # ti_orderid
            '',  # ti_sku
            '',  # ti_name
            '',  # ti_category
            '',  # ti_price
            '',  # ti_quantity
            '',  # pp_xoffset_min
            '',  # pp_xoffset_max
            '',  # pp_yoffset_min
            '',  # pp_yoffset_max
            user_agent,  # useragent
            '',  # br_name
            '',  # br_family
            '',  # br_version
            '',  # br_type
            '',  # br_renderengine
            'en-US',  # br_lang
            '',  # br_features_pdf
            '',  # br_features_flash
            '',  # br_features_java
            '',  # br_features_director
            '',  # br_features_quicktime
            '',  # br_features_realplayer
            '',  # br_features_windowsmedia
            '',  # br_features_gears
            '',  # br_features_silverlight
            'TRUE',  # br_cookies
            '24',  # br_colordepth
            str(random.randint(800, 1920)),  # br_viewwidth
            str(random.randint(600, 1080)),  # br_viewheight
            '',  # os_name
            '',  # os_family
            '',  # os_manufacturer
            'America/New_York',  # os_timezone
            '',  # dvce_type
            'TRUE' if 'Mobile' in user_agent else 'FALSE',  # dvce_ismobile
            str(random.randint(320, 1920)),  # dvce_screenwidth
            str(random.randint(568, 1080)),  # dvce_screenheight
            'UTF-8',  # doc_charset
            str(random.randint(800, 1920)),  # doc_width
            str(random.randint(600, 1080)),  # doc_height
            '',  # tr_currency
            '',  # tr_total_base
            '',  # tr_tax_base
            '',  # tr_shipping_base
            '',  # ti_currency
            '',  # ti_price_base
            '',  # base_currency
            'America/New_York',  # geo_timezone
            '',  # mkt_clickid
            '',  # mkt_network
            '',  # etl_tags
            dvce_created_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # dvce_sent_tstamp with milliseconds
            '',  # refr_domain_userid
            '',  # refr_dvce_tstamp
            domain_sessionid,  # domain_sessionid
            collector_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # derived_tstamp with milliseconds
            'com.snowplowanalytics.snowplow',  # event_vendor
            event_name,  # event_name
            'jsonschema',  # event_format
            '1-0-0',  # event_version
            str(uuid.uuid4()),  # event_fingerprint
            '',  # true_tstamp
            '',  # load_tstamp
            json.dumps(web_page_context),  # contexts_com_snowplowanalytics_snowplow_web_page_1
            '',  # unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1
            '',  # unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1
            json.dumps(iab_context),  # contexts_com_iab_snowplow_spiders_and_robots_1
            json.dumps(ua_context),  # contexts_com_snowplowanalytics_snowplow_ua_parser_context_1
            json.dumps(yauaa_context),  # contexts_nl_basjes_yauaa_context_1
            json.dumps(web_vitals)  # unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1
        ]
        
        events.append(event)
    
    return events

def get_csv_headers():
    """Return CSV headers for the events file."""
    return [
        'app_id', 'platform', 'etl_tstamp', 'collector_tstamp', 'dvce_created_tstamp', 'event', 'event_id', 'txn_id', 'name_tracker', 'v_tracker',
        'v_collector', 'v_etl', 'user_id', 'user_ipaddress', 'user_fingerprint', 'domain_userid', 'domain_sessionidx', 'network_userid', 'geo_country', 'geo_region',
        'geo_city', 'geo_zipcode', 'geo_latitude', 'geo_longitude', 'geo_region_name', 'ip_isp', 'ip_organization', 'ip_domain', 'ip_netspeed', 'page_url',
        'page_title', 'page_referrer', 'page_urlscheme', 'page_urlhost', 'page_urlport', 'page_urlpath', 'page_urlquery', 'page_urlfragment', 'refr_urlscheme', 'refr_urlhost',
        'refr_urlport', 'refr_urlpath', 'refr_urlquery', 'refr_urlfragment', 'refr_medium', 'refr_source', 'refr_term', 'mkt_medium', 'mkt_source', 'mkt_term',
        'mkt_content', 'mkt_campaign', 'se_category', 'se_action', 'se_label', 'se_property', 'se_value', 'tr_orderid', 'tr_affiliation', 'tr_total', 'tr_tax',
        'tr_shipping', 'tr_city', 'tr_state', 'tr_country', 'ti_orderid', 'ti_sku', 'ti_name', 'ti_category', 'ti_price', 'ti_quantity', 'pp_xoffset_min', 'pp_xoffset_max',
        'pp_yoffset_min', 'pp_yoffset_max', 'useragent', 'br_name', 'br_family', 'br_version', 'br_type', 'br_renderengine', 'br_lang', 'br_features_pdf', 'br_features_flash',
        'br_features_java', 'br_features_director', 'br_features_quicktime', 'br_features_realplayer', 'br_features_windowsmedia', 'br_features_gears', 'br_features_silverlight',
        'br_cookies', 'br_colordepth', 'br_viewwidth', 'br_viewheight', 'os_name', 'os_family', 'os_manufacturer', 'os_timezone', 'dvce_type', 'dvce_ismobile', 'dvce_screenwidth',
        'dvce_screenheight', 'doc_charset', 'doc_width', 'doc_height', 'tr_currency', 'tr_total_base', 'tr_tax_base', 'tr_shipping_base', 'ti_currency', 'ti_price_base',
        'base_currency', 'geo_timezone', 'mkt_clickid', 'mkt_network', 'etl_tags', 'dvce_sent_tstamp', 'refr_domain_userid', 'refr_dvce_tstamp', 'domain_sessionid', 'derived_tstamp',
        'event_vendor', 'event_name', 'event_format', 'event_version', 'event_fingerprint', 'true_tstamp', 'load_tstamp', 'contexts_com_snowplowanalytics_snowplow_web_page_1',
        'unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1', 'unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1',
        'contexts_com_iab_snowplow_spiders_and_robots_1', 'contexts_com_snowplowanalytics_snowplow_ua_parser_context_1', 'contexts_nl_basjes_yauaa_context_1',
        'unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1'
    ]

def write_events_csv(filename, events):
    """Write events to CSV file."""
    headers = get_csv_headers()
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(events)
    
    file_size = os.path.getsize(filename) / (1024 * 1024)  # Size in MB
    print(f"Generated {len(events)} events in {filename} ({file_size:.2f} MB)")

def generate_events_by_size(filename, target_date, target_size_gb):
    """
    Generate events to reach a target file size in GB.
    
    Args:
        filename: Output CSV filename
        target_date: Date for the events
        target_size_gb: Target file size in GB
    
    Returns:
        List of generated events
    """
    target_size_bytes = target_size_gb * 1024 * 1024 * 1024
    headers = get_csv_headers()
    
    # Generate a small sample to estimate row size
    sample_events = generate_event_data(target_date, num_events=100)
    
    # Write sample to temp file to measure size
    temp_file = f"{filename}.tmp"
    with open(temp_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(sample_events)
    
    sample_size = os.path.getsize(temp_file)
    header_size = len(','.join(headers)) + 1  # Approximate header size
    avg_row_size = (sample_size - header_size) / len(sample_events)
    
    # Calculate estimated number of rows needed
    estimated_rows = int((target_size_bytes - header_size) / avg_row_size)
    
    print(f"Estimated row size: {avg_row_size:.2f} bytes")
    print(f"Estimated rows needed for {target_size_gb} GB: {estimated_rows:,}")
    
    # Remove temp file
    os.remove(temp_file)
    
    # Generate events in batches
    batch_size = 10000
    all_events = []
    rows_generated = 0
    
    print(f"Generating events in batches of {batch_size:,}...")
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        
        while rows_generated < estimated_rows:
            # Generate batch
            remaining = estimated_rows - rows_generated
            current_batch_size = min(batch_size, remaining)
            
            batch_events = generate_event_data(target_date, num_events=current_batch_size)
            writer.writerows(batch_events)
            all_events.extend(batch_events)
            
            rows_generated += current_batch_size
            current_size = os.path.getsize(filename) / (1024 * 1024 * 1024)  # Size in GB
            
            if rows_generated % 50000 == 0 or rows_generated == estimated_rows:
                print(f"  Progress: {rows_generated:,} rows, {current_size:.3f} GB")
    
    final_size = os.path.getsize(filename) / (1024 * 1024 * 1024)
    print(f"✓ Generated {len(all_events):,} events in {filename} ({final_size:.3f} GB)")
    
    return all_events

def print_usage():
    """Print usage information."""
    print("Usage:")
    print("  python gen_events.py [--rows N | --gb SIZE]")
    print()
    print("Options:")
    print("  --rows N      Generate N rows per file (default: 1000)")
    print("  --gb SIZE     Generate files based on total size SIZE GB")
    print()
    print("Output files:")
    print("  - events_yesterday.csv: Half of total size (SIZE/2 GB)")
    print("  - events_today.csv: Half of total size (SIZE/2 GB)")
    print()
    print("Examples:")
    print("  python gen_events.py --rows 10000")
    print("  python gen_events.py --gb 1")
    print("    → events_yesterday.csv: 0.5 GB")
    print("    → events_today.csv: 0.5 GB")

def main():
    """Generate events files based on scale factor."""
    
    # Parse command line arguments
    use_size_mode = False
    scale_factor_gb = None
    num_events = 1000  # default
    
    if len(sys.argv) > 1:
        if sys.argv[1] in ['-h', '--help']:
            print_usage()
            return
        elif sys.argv[1] in ['--gb', '--scale-factor']:
            if len(sys.argv) < 3:
                print("Error: --gb requires a size argument")
                print_usage()
                sys.exit(1)
            try:
                scale_factor_gb = float(sys.argv[2])
                use_size_mode = True
            except ValueError:
                print(f"Error: '{sys.argv[2]}' is not a valid number")
                print_usage()
                sys.exit(1)
        elif sys.argv[1] == '--rows':
            if len(sys.argv) < 3:
                print("Error: --rows requires a number argument")
                print_usage()
                sys.exit(1)
            try:
                num_events = int(sys.argv[2])
            except ValueError:
                print(f"Error: '{sys.argv[2]}' is not a valid number")
                print_usage()
                sys.exit(1)
        else:
            # Backward compatibility: assume it's a row count
            try:
                num_events = int(sys.argv[1])
            except ValueError:
                print(f"Error: '{sys.argv[1]}' is not a valid number")
                print_usage()
                sys.exit(1)
    
    # Calculate dates
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    
    print("="*60)
    print("Snowplow Event Data Generator")
    print("="*60)

    if use_size_mode:
        print(f"Mode: Size-based generation")
        print(f"Total size: {scale_factor_gb} GB")
        print(f"Generating files:")
        print(f"  - events_yesterday.csv: {scale_factor_gb/2} GB")
        print(f"  - events_today.csv: {scale_factor_gb/2} GB")
        print()

        # Generate events_yesterday.csv (half of total size)
        print(f"Generating events_yesterday.csv ({scale_factor_gb/2} GB)...")
        yesterday_events = generate_events_by_size('events_yesterday.csv', yesterday, scale_factor_gb/2)

        print()
        # Generate events_today.csv (half of total size)
        print(f"Generating events_today.csv ({scale_factor_gb/2} GB)...")
        today_events = generate_events_by_size('events_today.csv', today, scale_factor_gb/2)
        
    else:
        print(f"Mode: Row-based generation")
        print(f"Rows per day: {num_events:,}")
        print(f"Generating events for:")
        print(f"  Yesterday: {yesterday}")
        print(f"  Today: {today}")
        print()

        # Generate events for yesterday
        print(f"Generating yesterday's events ({num_events:,} rows)...")
        yesterday_events = generate_event_data(yesterday, num_events=num_events)

        # Generate events for today
        print(f"Generating today's events ({num_events:,} rows)...")
        today_events = generate_event_data(today, num_events=num_events)

        # Create the two required files
        print(f"Creating events_yesterday.csv ({num_events:,} rows)...")
        write_events_csv('events_yesterday.csv', yesterday_events)

        print(f"Creating events_today.csv ({num_events:,} rows)...")
        write_events_csv('events_today.csv', today_events)
    
    print()
    print("="*60)
    print("✓ Generation complete!")
    print("="*60)
    print("Files generated:")
    if use_size_mode:
        print(f"  - events_yesterday.csv ({scale_factor_gb/2} GB)")
        print(f"  - events_today.csv ({scale_factor_gb/2} GB)")
        print(f"  Total: {scale_factor_gb} GB")
    else:
        print(f"  - events_yesterday.csv ({num_events:,} rows)")
        print(f"  - events_today.csv ({num_events:,} rows)")
        print(f"  Total: {num_events * 2:,} rows")
    print("="*60)

if __name__ == "__main__":
    main() 