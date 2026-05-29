import os
import sqlite3
import tempfile
import time

import requests


ACCOUNT_ID = os.environ.get('CLOUDFLARE_ACCOUNT_ID', '8460cfa72309d5c869775d6c38ca41dd')
DATABASE_ID = os.environ.get('TOPUS_D1_DATABASE_ID', '76daece6-bd81-4c16-954b-dfafce9691ae')
TIMESTAMP = os.environ.get('TOPUS_D1_TIMESTAMP', '2026-05-28T23:50:00Z')
API_BASE = 'https://api.cloudflare.com/client/v4'


def cf_request(method, path, **kwargs):
    token = os.environ['CLOUDFLARE_API_TOKEN']
    response = requests.request(
        method,
        f'{API_BASE}{path}',
        headers={
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
        },
        timeout=120,
        **kwargs,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get('success'):
        raise RuntimeError(payload)
    return payload.get('result') or payload


def bookmark_for_timestamp():
    result = cf_request(
        'GET',
        f'/accounts/{ACCOUNT_ID}/d1/database/{DATABASE_ID}/time_travel/bookmark',
        params={'timestamp': TIMESTAMP},
    )
    return result['bookmark']


def export_user_subscriptions(bookmark):
    current_bookmark = bookmark
    for _ in range(30):
        result = cf_request(
            'POST',
            f'/accounts/{ACCOUNT_ID}/d1/database/{DATABASE_ID}/export',
            json={
                'output_format': 'polling',
                'dump_options': {
                    'no_schema': False,
                    'no_data': False,
                    'tables': ['user_subscriptions'],
                },
                'current_bookmark': current_bookmark,
            },
        )
        print(f"export status={result.get('status')} at_bookmark={result.get('at_bookmark')} messages={result.get('messages')}")
        if result.get('status') == 'complete':
            signed_url = result['result']['signed_url']
            response = requests.get(signed_url, timeout=120)
            response.raise_for_status()
            return response.text
        if result.get('status') == 'error':
            raise RuntimeError(result)
        current_bookmark = result.get('at_bookmark') or current_bookmark
        time.sleep(2)
    raise TimeoutError('D1 export did not complete')


def load_dump(sql_text):
    connection = sqlite3.connect(':memory:')
    connection.executescript(sql_text)
    return connection


def print_summary(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''SELECT project_code, user_id, COUNT(*) total,
                  SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) active,
                  SUM(CASE WHEN active = 0 THEN 1 ELSE 0 END) inactive
           FROM user_subscriptions
           GROUP BY project_code, user_id
           ORDER BY active DESC'''
    )
    for row in cursor.fetchall():
        print('summary=' + repr(row))
    cursor.execute(
        '''SELECT project_code, user_id, channel_id, active, created_at, updated_at
           FROM user_subscriptions
           WHERE user_id = '106662708' AND active = 0
           ORDER BY channel_id'''
    )
    rows = cursor.fetchall()
    print(f'user_106662708_inactive={len(rows)}')
    for row in rows[:50]:
        print('inactive_106662708=' + repr(row))


def main():
    bookmark = bookmark_for_timestamp()
    print(f'timestamp={TIMESTAMP} bookmark={bookmark}')
    sql_text = export_user_subscriptions(bookmark)
    print(f'export_bytes={len(sql_text.encode("utf-8"))}')
    with tempfile.NamedTemporaryFile('w', suffix='.sql', delete=False, encoding='utf-8') as handle:
        handle.write(sql_text)
        print(f'temp_sql={handle.name}')
    connection = load_dump(sql_text)
    print_summary(connection)


if __name__ == '__main__':
    main()
