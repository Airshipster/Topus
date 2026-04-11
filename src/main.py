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
    print("DIAGNOSTIC MODE")
    print("="*60)
    
    print(f"\nCurrent UTC time: {datetime.utcnow().isoformat()}")
    print(f"Max video age: {MAX_VIDEO_AGE_HOURS} hours")
    
    cutoff_time = datetime.utcnow() - timedelta(hours=MAX_VIDEO_AGE_HOURS)
    print(f"Cutoff time: {cutoff_time.isoformat()}")
    print(f"Videos must be published AFTER: {cutoff_time.isoformat()}\n")
    
    test_channel = "UCmeHX75iiqezgdKgYfrFKSA"
    url = f"{CLOUDFLARE_WORKER_URL}/?channel={test_channel}"
    
    print(f"Fetching RSS for channel: {test_channel}")
    print(f"URL: {url}\n")
    
    response = requests.get(url, timeout=15)
    print(f"Response status: {response.status_code}")
    print(f"Response length: {len(response.content)} bytes\n")
    
    from xml.etree import ElementTree as ET
    root = ET.fromstring(response.content)
    
    ns = {
        'atom': 'http://www.w3.org/2005/Atom',
        'yt': 'http://www.youtube.com/xml/schemas/2015'
    }
    
    entries = root.findall('atom:entry', ns)
    print(f"Total entries found: {len(entries)}\n")
    print("="*60)
    
    for idx, entry in enumerate(entries, 1):
        video_id_elem = entry.find('yt:videoId', ns)
        title_elem = entry.find('atom:title', ns)
        published_elem = entry.find('atom:published', ns)
        
        if not all([video_id_elem, title_elem, published_elem]):
            continue
        
        video_id = video_id_elem.text
        title = title_elem.text
        published_str = published_elem.text
        
        print(f"\n[{idx}] {title[:50]}")
        print(f"    Video ID: {video_id}")
        print(f"    Published (raw): {published_str}")
        
        try:
            if published_str.endswith('Z'):
                published = datetime.fromisoformat(published_str.replace('Z', '+00:00')).replace(tzinfo=None)
            else:
                published = datetime.fromisoformat(published_str).replace(tzinfo=None)
            
            print(f"    Published (parsed): {published.isoformat()}")
            
            age_seconds = (datetime.utcnow() - published).total_seconds()
            age_hours = age_seconds / 3600
            age_days = age_hours / 24
            
            print(f"    Age: {age_hours:.1f} hours ({age_days:.1f} days)")
            
            passes = published > cutoff_time
            print(f"    Cutoff check: {published.isoformat()} > {cutoff_time.isoformat()}")
            print(f"    PASSES FILTER: {passes}")
            
        except Exception as e:
            print(f"    ERROR parsing date: {e}")
    
    print("\n" + "="*60)
    print("END DIAGNOSTIC")
    print("="*60)

if __name__ == "__main__":
    main()
