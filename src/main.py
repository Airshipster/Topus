import gspread
import requests
import json
import time
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from config import *

def authenticate_google_sheets():
    if not SERVICE_ACCOUNT_JSON:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON not found")
    credentials_dict = json.loads(SERVICE_ACCOUNT_JSON)
    credentials = Credentials.from_service_account_info(
        credentials_dict,
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    client = gspread.authorize(credentials)
    return client

def main():
    print("="*60)
    print("DIAGNOSTIC MODE - RSS DATE ANALYSIS")
    print("="*60)
    
    print(f"\nCurrent UTC time: {datetime.utcnow().isoformat()}")
    print(f"Max video age setting: {MAX_VIDEO_AGE_HOURS} hours")
    
    cutoff_time = datetime.utcnow() - timedelta(hours=MAX_VIDEO_AGE_HOURS)
    print(f"Cutoff time (videos must be AFTER this): {cutoff_time.isoformat()}")
    print(f"That is: {MAX_VIDEO_AGE_HOURS/24:.1f} days ago\n")
    
    test_channel = "UCmeHX75iiqezgdKgYfrFKSA"
    url = f"{CLOUDFLARE_WORKER_URL}/?channel={test_channel}"
    
    print(f"Testing channel: {test_channel}")
    print(f"Cloudflare URL: {url}\n")
    
    response = requests.get(url, timeout=15)
    print(f"HTTP Status: {response.status_code}")
    print(f"Response size: {len(response.content)} bytes\n")
    
    from xml.etree import ElementTree as ET
    root = ET.fromstring(response.content)
    
    ns = {
        'atom': 'http://www.w3.org/2005/Atom',
        'yt': 'http://www.youtube.com/xml/schemas/2015'
    }
    
    entries = root.findall('atom:entry', ns)
    print(f"Total videos in feed: {len(entries)}\n")
    print("="*60)
    print("VIDEO ANALYSIS:")
    print("="*60)
    
    passed = 0
    failed = 0
    
    for idx, entry in enumerate(entries, 1):
        video_id_elem = entry.find('yt:videoId', ns)
        title_elem = entry.find('atom:title', ns)
        published_elem = entry.find('atom:published', ns)
        
        print(f"\n[{idx}] Elements found: video={video_id_elem is not None}, title={title_elem is not None}, pub={published_elem is not None}")
        
        if video_id_elem is not None and video_id_elem.text:
            video_id = video_id_elem.text
            print(f"    Video ID: {video_id}")
        else:
            print(f"    Video ID: MISSING or EMPTY")
            continue
        
        if title_elem is not None and title_elem.text:
            title = title_elem.text
            print(f"    Title: {title[:55]}")
        else:
            print(f"    Title: MISSING or EMPTY")
            continue
        
        if published_elem is not None and published_elem.text:
            published_str = published_elem.text
            print(f"    Raw date: {published_str}")
        else:
            print(f"    Published: MISSING or EMPTY")
            continue
        
        try:
            if published_str.endswith('Z'):
                published = datetime.fromisoformat(published_str.replace('Z', '+00:00')).replace(tzinfo=None)
            else:
                published = datetime.fromisoformat(published_str).replace(tzinfo=None)
            
            print(f"    Parsed: {published.isoformat()}")
            
            age_seconds = (datetime.utcnow() - published).total_seconds()
            age_hours = age_seconds / 3600
            age_days = age_hours / 24
            
            print(f"    Age: {age_hours:.1f}h = {age_days:.1f}d")
            
            passes = published > cutoff_time
            
            if passes:
                print(f"    ✅ PASSES")
                passed += 1
            else:
                print(f"    ❌ TOO OLD")
                failed += 1
            
        except Exception as e:
            print(f"    ⚠️  ERROR: {e}")
            failed += 1
    
    print("\n" + "="*60)
    print("SUMMARY:")
    print("="*60)
    print(f"Total: {len(entries)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Window: {MAX_VIDEO_AGE_HOURS}h ({MAX_VIDEO_AGE_HOURS/24:.1f}d)")
    print("="*60)

if __name__ == "__main__":
    main()
