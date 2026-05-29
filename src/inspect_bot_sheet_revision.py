import io
import json
import os
import re
import zipfile
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import requests
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials

import config


DRIVE_SCOPE = 'https://www.googleapis.com/auth/drive.readonly'
XLSX_MIME = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
SHEET_NAME = 'Боты'


def service_account_token():
    info = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])
    credentials = Credentials.from_service_account_info(info, scopes=[DRIVE_SCOPE])
    credentials.refresh(Request())
    return credentials.token


def drive_get(token, path, **params):
    response = requests.get(
        f'https://www.googleapis.com/drive/v3/{path}',
        headers={'Authorization': f'Bearer {token}'},
        params=params,
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def list_revisions(token):
    revisions = []
    page_token = ''
    while True:
        payload = drive_get(
            token,
            f'files/{config.SPREADSHEET_ID}/revisions',
            pageSize=200,
            pageToken=page_token or None,
            fields='nextPageToken,revisions(id,modifiedTime,mimeType,keepForever)',
        )
        revisions.extend(payload.get('revisions') or [])
        page_token = payload.get('nextPageToken') or ''
        if not page_token:
            return revisions


def parse_rfc3339(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def choose_revision(revisions):
    before_text = os.environ.get('TOPUS_REVISION_BEFORE', '2026-05-28T23:50:00Z')
    before = parse_rfc3339(before_text)
    candidates = [
        revision for revision in revisions
        if parse_rfc3339(revision['modifiedTime']) <= before
    ]
    if not candidates:
        raise RuntimeError(f'No revision before {before_text}')
    return max(candidates, key=lambda revision: revision['modifiedTime'])


def download_revision_xlsx(token, revision_id):
    metadata = drive_get(
        token,
        f'files/{config.SPREADSHEET_ID}/revisions/{revision_id}',
        fields='id,modifiedTime,mimeType,exportLinks',
    )
    export_link = (metadata.get('exportLinks') or {}).get(XLSX_MIME)
    if not export_link:
        raise RuntimeError(f'No xlsx export link for revision {revision_id}: {metadata}')
    response = requests.get(export_link, headers={'Authorization': f'Bearer {token}'}, timeout=120)
    response.raise_for_status()
    return metadata, response.content


def xml_text(element):
    return ''.join(element.itertext()) if element is not None else ''


def parse_xlsx_sheet_rows(content, sheet_name):
    archive = zipfile.ZipFile(io.BytesIO(content))
    ns = {
        'main': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main',
        'rel': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships',
        'pkgrel': 'http://schemas.openxmlformats.org/package/2006/relationships',
    }

    shared_strings = []
    if 'xl/sharedStrings.xml' in archive.namelist():
        root = ET.fromstring(archive.read('xl/sharedStrings.xml'))
        for item in root.findall('main:si', ns):
            shared_strings.append(xml_text(item))

    workbook = ET.fromstring(archive.read('xl/workbook.xml'))
    rels = ET.fromstring(archive.read('xl/_rels/workbook.xml.rels'))
    rel_targets = {
        rel.attrib['Id']: rel.attrib['Target']
        for rel in rels.findall('pkgrel:Relationship', ns)
    }
    sheet_path = None
    for sheet in workbook.findall('main:sheets/main:sheet', ns):
        if sheet.attrib.get('name') == sheet_name:
            rel_id = sheet.attrib.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
            target = rel_targets[rel_id]
            sheet_path = 'xl/' + target.lstrip('/')
            break
    if not sheet_path:
        raise RuntimeError(f'Sheet not found: {sheet_name}')

    root = ET.fromstring(archive.read(sheet_path))
    rows = []
    for row in root.findall('main:sheetData/main:row', ns):
        values = []
        for cell in row.findall('main:c', ns):
            ref = cell.attrib.get('r', '')
            match = re.match(r'([A-Z]+)', ref)
            if match:
                col = 0
                for char in match.group(1):
                    col = col * 26 + ord(char) - 64
                while len(values) < col - 1:
                    values.append('')
            value_node = cell.find('main:v', ns)
            inline_node = cell.find('main:is', ns)
            raw = xml_text(inline_node) if inline_node is not None else xml_text(value_node)
            if cell.attrib.get('t') == 's' and raw:
                raw = shared_strings[int(raw)]
            values.append(raw)
        rows.append(values)
    return rows


def main():
    token = service_account_token()
    revisions = list_revisions(token)
    print(f'revisions={len(revisions)}')
    for revision in revisions[-12:]:
        print(f"revision id={revision.get('id')} modified={revision.get('modifiedTime')} mime={revision.get('mimeType')}")
    revision = choose_revision(revisions)
    print(f"chosen id={revision['id']} modified={revision['modifiedTime']}")
    metadata, content = download_revision_xlsx(token, revision['id'])
    print(f"downloaded revision modified={metadata.get('modifiedTime')} bytes={len(content)}")
    rows = parse_xlsx_sheet_rows(content, SHEET_NAME)
    print(f'rows={len(rows)}')
    headers = rows[0] if rows else []
    for index, header in enumerate(headers[:22], start=1):
        print(f'header {index}: {header!r}')
    for row in rows[1:12]:
        padded = row + [''] * 22
        print('row=' + repr(padded[:19]))


if __name__ == '__main__':
    main()
