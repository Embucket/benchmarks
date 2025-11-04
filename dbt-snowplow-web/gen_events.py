#!/usr/bin/env python3
"""
Script to generate events.csv files with Snowplow event data for yesterday and today.
Can generate based on target GB size instead of row count.
"""

import csv
import uuid
import random
from datetime import datetime, timedelta
from urllib.parse import urlparse
import json
import os
import sys

def generate_event_data(target_date, num_events=1000, mobile_percentage=50):
    """Generate sample Snowplow event data for a specific date.

    For each page view, generates:
    - 1 page_view event
    - Multiple page_ping events (for engagement tracking)

    Args:
        target_date: Date for the events
        num_events: Number of page views to generate
        mobile_percentage: Percentage of events that should be mobile (0-100)
    """

    # Sample data for variety
    countries = ['US', 'CA', 'GB', 'DE', 'FR', 'JP', 'AU', 'BR', 'IN', 'MX']
    cities = ['New York', 'London', 'Berlin', 'Paris', 'Tokyo', 'Sydney', 'São Paulo', 'Mumbai', 'Mexico City']

    # Desktop user agents
    desktop_user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    ]

    # Mobile user agents
    mobile_user_agents = [
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/94.0.4606.76 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Mobile Safari/537.36',
    ]

    pages = [
        'https://example.com/home',
        'https://example.com/products',
        'https://example.com/about',
        'https://example.com/contact',
        'https://example.com/blog'
    ]

    events = []

    for i in range(num_events):
        # Generate timestamps for the target date
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        base_time = datetime.combine(target_date, datetime.min.time().replace(hour=hour, minute=minute, second=second))
        # Add milliseconds for compatibility with dbt models
        base_time = base_time.replace(microsecond=random.randint(0, 999999))

        # Generate event data
        user_id = str(uuid.uuid4())
        domain_userid = str(uuid.uuid4())
        domain_sessionid = str(uuid.uuid4())
        network_userid = str(uuid.uuid4())
        page_view_id = str(uuid.uuid4())  # Unique ID for this page view

        country = random.choice(countries)
        city = random.choice(cities)

        # Select user agent based on mobile_percentage
        is_mobile = random.randint(1, 100) <= mobile_percentage
        user_agent = random.choice(mobile_user_agents if is_mobile else desktop_user_agents)

        page_url = random.choice(pages)

        # Generate page_view event first
        collector_tstamp = base_time
        dvce_created_tstamp = base_time - timedelta(seconds=random.randint(1, 5), microseconds=random.randint(0, 999999))
        etl_tstamp = base_time + timedelta(seconds=random.randint(1, 3), microseconds=random.randint(0, 999999))
        event_id = str(uuid.uuid4())
        
        # Generate contexts (simplified JSON)
        ua_context = [{
            'deviceFamily': 'iPhone' if 'iPhone' in user_agent else 'Desktop',
            'osFamily': 'iOS' if 'iPhone' in user_agent else 'Windows',
            'useragentFamily': 'Safari' if 'Safari' in user_agent else 'Chrome'
        }]

        # Use the same page_view_id for all events related to this page view
        web_page_context = [{'id': page_view_id}]
        iab_context = [{'category': 'BROWSER', 'spiderOrRobot': False}]
        yauaa_context = [{'agentClass': 'Browser', 'deviceClass': 'Phone' if 'Mobile' in user_agent else 'Desktop'}]
        
        # Generate web vitals (following SNOWPLOW_DATA_GUIDE.md)
        web_vitals = [{
            'cls': round(random.uniform(0.0, 0.25), 3),  # Cumulative Layout Shift (0-1, typically 0-0.25 for good pages)
            'fcp': random.randint(500, 3000),  # First Contentful Paint (ms)
            'fid': random.randint(1, 300),  # First Input Delay (ms)
            'inp': random.randint(1, 300),  # Interaction to Next Paint (ms)
            'lcp': random.randint(1000, 4000),  # Largest Contentful Paint (ms)
            'navigation_type': 'navigate',  # Navigation type
            'ttfb': random.randint(50, 500)  # Time to First Byte (ms)
        }]

        # Create page_view event
        page_view_event = [
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
            'page_view',  # event_name - always page_view for this event
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
            ''  # unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1 (web_vitals is a separate event)
        ]

        events.append(page_view_event)

        # Generate cmp_visible event for some sessions (typically early in session, before consent)
        # CMP visible events occur 35% of the time, right after page_view
        if random.randint(1, 100) <= 35:
            cmp_delay_ms = random.randint(50, 300)  # CMP banner appears shortly after page_view
            cmp_time = base_time + timedelta(microseconds=cmp_delay_ms * 1000)
            cmp_collector_tstamp = cmp_time
            cmp_dvce_created_tstamp = cmp_time - timedelta(microseconds=random.randint(1, 20) * 1000)
            cmp_etl_tstamp = cmp_time + timedelta(microseconds=random.randint(1, 20) * 1000)
            cmp_event_id = str(uuid.uuid4())
            
            # Generate CMP visible data (simple structure with elapsed_time as string)
            cmp_visible = [{
                'elapsed_time': str(round(random.uniform(0.5, 3.0), 1))
            }]
            
            cmp_event = [
                'default',  # app_id
                'web',      # platform
                cmp_etl_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # etl_tstamp
                cmp_collector_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # collector_tstamp
                cmp_dvce_created_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # dvce_created_tstamp
                'unstruct',  # event - unstruct for cmp_visible
                cmp_event_id,  # event_id
                '',  # txn_id
                'eng.gcp-dev1',  # name_tracker
                'js-2.17.2',  # v_tracker
                'ssc-2.1.2-googlepubsub',  # v_collector
                'beam-enrich-1.4.2-rc1-common-1.4.2-rc1',  # v_etl
                user_id,  # user_id (same as page_view)
                '',  # user_ipaddress
                str(uuid.uuid4()),  # user_fingerprint
                domain_userid,  # domain_userid (same as page_view)
                '1',  # domain_sessionidx
                network_userid,  # network_userid (same as page_view)
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
                page_url,  # page_url (same as page_view)
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
                cmp_dvce_created_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # dvce_sent_tstamp
                '',  # refr_domain_userid
                '',  # refr_dvce_tstamp
                domain_sessionid,  # domain_sessionid (same as page_view)
                cmp_collector_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # derived_tstamp
                'com.snowplowanalytics.snowplow',  # event_vendor
                'cmp_visible',  # event_name - cmp_visible for unstruct event
                'jsonschema',  # event_format
                '1-0-0',  # event_version
                str(uuid.uuid4()),  # event_fingerprint
                '',  # true_tstamp
                '',  # load_tstamp
                json.dumps(web_page_context),  # contexts_com_snowplowanalytics_snowplow_web_page_1 (SAME page_view_id!)
                '',  # unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1
                json.dumps(cmp_visible),  # unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1
                json.dumps(iab_context),  # contexts_com_iab_snowplow_spiders_and_robots_1
                json.dumps(ua_context),  # contexts_com_snowplowanalytics_snowplow_ua_parser_context_1
                json.dumps(yauaa_context),  # contexts_nl_basjes_yauaa_context_1
                ''  # unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1
            ]
            
            events.append(cmp_event)

        # Generate consent_preferences event for some sessions (typically early in session)
        # Consent events occur 30% of the time, right after page_view
        if random.randint(1, 100) <= 30:
            consent_delay_ms = random.randint(100, 500)  # Consent happens shortly after page_view
            consent_time = base_time + timedelta(microseconds=consent_delay_ms * 1000)
            consent_collector_tstamp = consent_time
            consent_dvce_created_tstamp = consent_time - timedelta(microseconds=random.randint(1, 20) * 1000)
            consent_etl_tstamp = consent_time + timedelta(microseconds=random.randint(1, 20) * 1000)
            consent_event_id = str(uuid.uuid4())
            
            # Generate consent preferences data
            consent_scopes = random.choice([
                ['necessary'],
                ['necessary', 'preferences'],
                ['necessary', 'preferences', 'statistics'],
                ['necessary', 'preferences', 'statistics', 'marketing']
            ])
            consent_event_type = random.choice(['allow_all', 'allow_selected', 'deny_all'])
            
            # Extract base domain from page_url for domains_applied
            parsed_url = urlparse(page_url)
            base_domain = f"{parsed_url.scheme}://{parsed_url.netloc}/"
            
            consent_preferences = [{
                'basis_for_processing': 'consent',
                'consent_scopes': consent_scopes,
                'consent_url': page_url,
                'consent_version': '1.0',
                'domains_applied': [base_domain],
                'event_type': consent_event_type,
                'gdpr_applies': random.choice([True, False])
            }]
            
            consent_event = [
                'default',  # app_id
                'web',      # platform
                consent_etl_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # etl_tstamp
                consent_collector_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # collector_tstamp
                consent_dvce_created_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # dvce_created_tstamp
                'unstruct',  # event - unstruct for consent_preferences
                consent_event_id,  # event_id
                '',  # txn_id
                'eng.gcp-dev1',  # name_tracker
                'js-2.17.2',  # v_tracker
                'ssc-2.1.2-googlepubsub',  # v_collector
                'beam-enrich-1.4.2-rc1-common-1.4.2-rc1',  # v_etl
                user_id,  # user_id (same as page_view)
                '',  # user_ipaddress
                str(uuid.uuid4()),  # user_fingerprint
                domain_userid,  # domain_userid (same as page_view)
                '1',  # domain_sessionidx
                network_userid,  # network_userid (same as page_view)
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
                page_url,  # page_url (same as page_view)
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
                consent_dvce_created_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # dvce_sent_tstamp
                '',  # refr_domain_userid
                '',  # refr_dvce_tstamp
                domain_sessionid,  # domain_sessionid (same as page_view)
                consent_collector_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # derived_tstamp
                'com.snowplowanalytics.snowplow',  # event_vendor
                'consent_preferences',  # event_name - consent_preferences for unstruct event
                'jsonschema',  # event_format
                '1-0-0',  # event_version
                str(uuid.uuid4()),  # event_fingerprint
                '',  # true_tstamp
                '',  # load_tstamp
                json.dumps(web_page_context),  # contexts_com_snowplowanalytics_snowplow_web_page_1 (SAME page_view_id!)
                json.dumps(consent_preferences),  # unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1
                '',  # unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1
                json.dumps(iab_context),  # contexts_com_iab_snowplow_spiders_and_robots_1
                json.dumps(ua_context),  # contexts_com_snowplowanalytics_snowplow_ua_parser_context_1
                json.dumps(yauaa_context),  # contexts_nl_basjes_yauaa_context_1
                ''  # unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1
            ]
            
            events.append(consent_event)

        # Generate web_vitals event as separate unstruct event
        # Web vitals occur 100-2000ms after page_view (measures page load performance)
        vitals_delay_ms = random.randint(100, 2000)
        vitals_time = base_time + timedelta(microseconds=vitals_delay_ms * 1000)
        vitals_collector_tstamp = vitals_time
        vitals_dvce_created_tstamp = vitals_time - timedelta(microseconds=random.randint(1, 50) * 1000)
        vitals_etl_tstamp = vitals_time + timedelta(microseconds=random.randint(1, 50) * 1000)
        vitals_event_id = str(uuid.uuid4())

        web_vitals_event = [
            'default',  # app_id
            'web',      # platform
            vitals_etl_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # etl_tstamp
            vitals_collector_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # collector_tstamp
            vitals_dvce_created_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # dvce_created_tstamp
            'unstruct',  # event - unstruct for web_vitals
            vitals_event_id,  # event_id
            '',  # txn_id
            'eng.gcp-dev1',  # name_tracker
            'js-2.17.2',  # v_tracker
            'ssc-2.1.2-googlepubsub',  # v_collector
            'beam-enrich-1.4.2-rc1-common-1.4.2-rc1',  # v_etl
            user_id,  # user_id (same as page_view)
            '',  # user_ipaddress
            str(uuid.uuid4()),  # user_fingerprint
            domain_userid,  # domain_userid (same as page_view)
            '1',  # domain_sessionidx
            network_userid,  # network_userid (same as page_view)
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
            page_url,  # page_url (same as page_view)
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
            vitals_dvce_created_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # dvce_sent_tstamp
            '',  # refr_domain_userid
            '',  # refr_dvce_tstamp
            domain_sessionid,  # domain_sessionid (same as page_view)
            vitals_collector_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # derived_tstamp
            'com.snowplowanalytics.snowplow',  # event_vendor
            'web_vitals',  # event_name - web_vitals for unstruct event
            'jsonschema',  # event_format
            '1-0-0',  # event_version
            str(uuid.uuid4()),  # event_fingerprint
            '',  # true_tstamp
            '',  # load_tstamp
            json.dumps(web_page_context),  # contexts_com_snowplowanalytics_snowplow_web_page_1 (SAME page_view_id!)
            '',  # unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1
            '',  # unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1
            json.dumps(iab_context),  # contexts_com_iab_snowplow_spiders_and_robots_1
            json.dumps(ua_context),  # contexts_com_snowplowanalytics_snowplow_ua_parser_context_1
            json.dumps(yauaa_context),  # contexts_nl_basjes_yauaa_context_1
            json.dumps(web_vitals)  # unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1
        ]

        events.append(web_vitals_event)

        # Generate page_ping events for engagement tracking
        # Snowplow sends page_ping every 10 seconds by default (heartbeat)
        # Generate 0-12 pings (0-120 seconds of engagement)
        num_pings = random.randint(0, 12)

        for ping_num in range(num_pings):
            # Each ping is 10 seconds apart
            ping_time = base_time + timedelta(seconds=10 * (ping_num + 1))
            ping_collector_tstamp = ping_time
            ping_dvce_created_tstamp = ping_time - timedelta(seconds=random.randint(1, 3))
            ping_etl_tstamp = ping_time + timedelta(seconds=random.randint(1, 2))
            ping_event_id = str(uuid.uuid4())

            page_ping_event = [
                'default',  # app_id
                'web',      # platform
                ping_etl_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # etl_tstamp
                ping_collector_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # collector_tstamp
                ping_dvce_created_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # dvce_created_tstamp
                'page_ping',  # event - THIS IS THE KEY DIFFERENCE
                ping_event_id,  # event_id
                '',  # txn_id
                'eng.gcp-dev1',  # name_tracker
                'js-2.17.2',  # v_tracker
                'ssc-2.1.2-googlepubsub',  # v_collector
                'beam-enrich-1.4.2-rc1-common-1.4.2-rc1',  # v_etl
                user_id,  # user_id (same as page_view)
                '',  # user_ipaddress
                str(uuid.uuid4()),  # user_fingerprint
                domain_userid,  # domain_userid (same as page_view)
                '1',  # domain_sessionidx
                network_userid,  # network_userid (same as page_view)
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
                page_url,  # page_url (same as page_view)
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
                ping_dvce_created_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # dvce_sent_tstamp
                '',  # refr_domain_userid
                '',  # refr_dvce_tstamp
                domain_sessionid,  # domain_sessionid (same as page_view)
                ping_collector_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # derived_tstamp
                'com.snowplowanalytics.snowplow',  # event_vendor
                'page_ping',  # event_name - page_ping for engagement
                'jsonschema',  # event_format
                '1-0-0',  # event_version
                str(uuid.uuid4()),  # event_fingerprint
                '',  # true_tstamp
                                        '',  # load_tstamp
            json.dumps(web_page_context),  # contexts_com_snowplowanalytics_snowplow_web_page_1 (SAME page_view_id!)
            '',  # unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1
            '',  # unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1
            json.dumps(iab_context),  # contexts_com_iab_snowplow_spiders_and_robots_1
            json.dumps(ua_context),  # contexts_com_snowplowanalytics_snowplow_ua_parser_context_1
            json.dumps(yauaa_context),  # contexts_nl_basjes_yauaa_context_1
            ''  # unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1 (no web vitals for pings)
            ]

            events.append(page_ping_event)
    
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

def generate_events_by_size(filename, target_date, target_size_gb, mobile_percentage=50):
    """
    Generate events to reach a target file size in GB.

    Args:
        filename: Output CSV filename
        target_date: Date for the events
        target_size_gb: Target file size in GB
        mobile_percentage: Percentage of events that should be mobile (0-100)

    Returns:
        List of generated events
    """
    target_size_bytes = target_size_gb * 1024 * 1024 * 1024
    headers = get_csv_headers()

    # Generate a small sample to estimate row size
    # Note: generate_event_data returns ALL events (page_view + page_pings)
    sample_events = generate_event_data(target_date, num_events=100, mobile_percentage=mobile_percentage)

    # Write sample to temp file to measure size
    temp_file = f"{filename}.tmp"
    with open(temp_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(sample_events)

    sample_size = os.path.getsize(temp_file)
    header_size = len(','.join(headers)) + 1  # Approximate header size
    # FIXED: Use actual number of rows written, not num_events parameter
    actual_rows_in_sample = len(sample_events)
    avg_row_size = (sample_size - header_size) / actual_rows_in_sample

    # Calculate estimated number of PAGE VIEWS needed (not total rows)
    # Each page view generates ~7 events on average (1 page_view + ~6 page_pings)
    avg_events_per_page_view = actual_rows_in_sample / 100  # 100 was the num_events
    estimated_total_rows = int((target_size_bytes - header_size) / avg_row_size)
    estimated_page_views = int(estimated_total_rows / avg_events_per_page_view)

    print(f"Sample stats:")
    print(f"  - 100 page views generated {actual_rows_in_sample} total events")
    print(f"  - Average {avg_events_per_page_view:.1f} events per page view")
    print(f"  - Average row size: {avg_row_size:.2f} bytes")
    print(f"Estimated page views needed for {target_size_gb} GB: {estimated_page_views:,}")
    print(f"Estimated total rows: {estimated_total_rows:,}")

    # Remove temp file
    os.remove(temp_file)

    # Generate events in batches
    batch_size = 1000  # Page views per batch (will generate ~7x this many rows)
    all_events = []
    page_views_generated = 0

    print(f"Generating page views in batches of {batch_size:,}...")

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)

        while page_views_generated < estimated_page_views:
            # Generate batch
            remaining = estimated_page_views - page_views_generated
            current_batch_size = min(batch_size, remaining)

            batch_events = generate_event_data(target_date, num_events=current_batch_size, mobile_percentage=mobile_percentage)
            writer.writerows(batch_events)
            all_events.extend(batch_events)

            page_views_generated += current_batch_size
            current_size = os.path.getsize(filename) / (1024 * 1024 * 1024)  # Size in GB

            if page_views_generated % 5000 == 0 or page_views_generated == estimated_page_views:
                print(f"  Progress: {page_views_generated:,} page views ({len(all_events):,} total events), {current_size:.3f} GB")
    
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
    
    # Set specific dates: November 1st (yesterday) and November 2nd (today)
    yesterday = datetime(2025, 11, 1).date()  # November 1st
    today = datetime(2025, 11, 2).date()  # November 2nd
    
    print("="*60)
    print("Snowplow Event Data Generator")
    print("="*60)

    if use_size_mode:
        print(f"Mode: Size-based generation")
        print(f"Total size: {scale_factor_gb} GB")
        print(f"Generating files:")
        print(f"  - events_yesterday.csv: {scale_factor_gb/2} GB (66% mobile)")
        print(f"  - events_today.csv: {scale_factor_gb/2} GB (50% mobile)")
        print()

        # Generate events_yesterday.csv (half of total size, 66% mobile)
        print(f"Generating events_yesterday.csv ({scale_factor_gb/2} GB, 66% mobile)...")
        yesterday_events = generate_events_by_size('events_yesterday.csv', yesterday, scale_factor_gb/2, mobile_percentage=66)

        print()
        # Generate events_today.csv (half of total size, 50% mobile)
        print(f"Generating events_today.csv ({scale_factor_gb/2} GB, 50% mobile)...")
        today_events = generate_events_by_size('events_today.csv', today, scale_factor_gb/2, mobile_percentage=50)
        
    else:
        print(f"Mode: Row-based generation")
        print(f"Rows per day: {num_events:,}")
        print(f"Generating events for:")
        print(f"  Yesterday: {yesterday} (66% mobile)")
        print(f"  Today: {today} (50% mobile)")
        print()

        # Generate events for yesterday (66% mobile)
        print(f"Generating yesterday's events ({num_events:,} page views, 66% mobile)...")
        yesterday_events = generate_event_data(yesterday, num_events=num_events, mobile_percentage=66)

        # Generate events for today (50% mobile)
        print(f"Generating today's events ({num_events:,} page views, 50% mobile)...")
        today_events = generate_event_data(today, num_events=num_events, mobile_percentage=50)

        # Create the two required files
        print(f"Creating events_yesterday.csv ({len(yesterday_events):,} total events)...")
        write_events_csv('events_yesterday.csv', yesterday_events)

        print(f"Creating events_today.csv ({len(today_events):,} total events)...")
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