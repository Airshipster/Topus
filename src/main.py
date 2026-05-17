import time
import os
from datetime import datetime

from gspread.exceptions import APIError

import config
from filters import should_filter_video
from rss import rss_fallback_check
from sheets import (
    acquire_lock,
    authenticate_google_sheets,
    clean_master_numeric_text_values,
    cleanup_old_records,
    current_local_datetime,
    format_timestamp,
    clean_known_workbook_text_values,
    delete_stale_unpublished_video_rows,
    ensure_non_settings_sheet_row_counts,
    get_recent_published_video_rows,
    get_published_videos,
    get_push_events,
    load_projects,
    load_settings,
    load_youtube_channels,
    log_events_batch,
    mark_push_events_processed_batch,
    maintain_workbook_layout,
    reconcile_pending_published_videos,
    repair_video_publication_status,
    update_video_project_links,
    release_lock,
    save_videos_batch,
    update_project_channel_counts,
    update_video_publication_status,
    update_last_run,
    update_run_status,
    update_youtube_quota,
    parse_datetime_value,
)
from subscriptions import deduplicate_subscription_rows, get_subscription_records, sync_subscriptions
from telegram_client import delete_telegram_message, format_message, send_to_telegram
from youtube_client import get_last_youtube_api_error, get_video_info_from_api, get_youtube_api_calls


def parse_datetime(value):
    return parse_datetime_value(value)


def get_stale_reason(published_at, project=None):
    published = parse_datetime(published_at)
    if not published:
        return ''

    limit_hours = config.MAX_PUBLISH_AGE_HOURS
    if project:
        limit_hours = project.get('max_publish_age_hours') or limit_hours

    age_hours = (current_local_datetime() - published).total_seconds() / 3600
    if age_hours > limit_hours:
        return f"Stale video ({age_hours:.1f}h old, limit {limit_hours}h)"

    return ''


def copy_video_classification(video, video_info):
    if not video_info:
        return video
    for field in ('is_short', 'short_reason', 'is_live', 'duration', 'duration_seconds', 'width', 'height'):
        if field in video_info:
            video[field] = video_info[field]
    return video


def publication_status_detail(video):
    labels = []
    if video.get('is_short'):
        labels.append('Shorts')
    if video.get('is_live'):
        labels.append('Stream')
    return '. '.join(labels) + ('.' if labels else '')


def should_force_subscription_sync():
    value = os.environ.get('TOPUS_FORCE_SUBSCRIPTION_SYNC', '')
    return value.lower() in ('1', 'true', 'yes')


def sync_only_mode():
    value = os.environ.get('TOPUS_SYNC_ONLY', '')
    return value.lower() in ('1', 'true', 'yes')


def push_only_mode():
    value = os.environ.get('TOPUS_PUSH_ONLY', '')
    return value.lower() in ('1', 'true', 'yes')


def maintenance_only_mode():
    value = os.environ.get('TOPUS_MAINTENANCE_ONLY', '')
    return value.lower() in ('1', 'true', 'yes')


def repair_pending_only_mode():
    value = os.environ.get('TOPUS_REPAIR_PENDING_ONLY', '')
    return value.lower() in ('1', 'true', 'yes')


def repair_video_id():
    return os.environ.get('TOPUS_REPAIR_VIDEO_ID', '').strip()


def unlock_only_mode():
    value = os.environ.get('TOPUS_UNLOCK_ONLY', '')
    return value.lower() in ('1', 'true', 'yes')


def run_mode_name():
    if unlock_only_mode():
        return 'unlock-only'
    if repair_video_id():
        return 'repair-video'
    if repair_pending_only_mode():
        return 'repair-pending-only'
    if maintenance_only_mode():
        return 'maintenance-only'
    if sync_only_mode():
        return 'sync-only'
    if push_only_mode():
        return 'push-only'
    return 'scheduled'


def run_status_details():
    run_id = os.environ.get('GITHUB_RUN_ID', 'local')
    event = os.environ.get('GITHUB_EVENT_NAME', 'local')
    sha = os.environ.get('GITHUB_SHA', '')[:7]
    bits = [f'mode={run_mode_name()}', f'event={event}', f'run={run_id}']
    if sha:
        bits.append(f'sha={sha}')
    return ', '.join(bits)


def print_detection_latency_note():
    rss_avg_minutes = 15
    push_fallback_avg_minutes = 2.5
    push_wait_reduction = round((rss_avg_minutes - push_fallback_avg_minutes) / rss_avg_minutes * 100)
    print(
        "\n⏱️  Detection latency: Push API ≈0-5m fallback "
        f"(avg {push_fallback_avg_minutes:g}m), RSS feed ≈0-30m "
        f"(avg {rss_avg_minutes:g}m). Push API reduces waiting by ~{push_wait_reduction}% vs RSS-only."
    )


def publication_key(video_id, project):
    return (video_id, project['name'])


def acquire_lock_with_wait(master_sheet):
    if maintenance_only_mode():
        return acquire_lock(master_sheet, stale_after_seconds=120)

    if push_only_mode():
        return acquire_lock(master_sheet)

    for attempt in range(1, 21):
        if acquire_lock(master_sheet):
            return True

        wait_seconds = 15
        print(f"  ⏳ Lock busy, waiting {wait_seconds}s before retry {attempt}/20...")
        time.sleep(wait_seconds)

    return False


def is_sheets_quota_error(error):
    return isinstance(error, APIError) and '[429]' in str(error)


def open_master_sheet_with_retry(client, attempts=4):
    for attempt in range(1, attempts + 1):
        try:
            return client.open_by_key(config.SPREADSHEET_ID)
        except Exception as error:
            if not is_sheets_quota_error(error) or attempt == attempts:
                raise
            wait_seconds = attempt * 15
            print(f"  ⏳ Google Sheets quota busy while opening master sheet; retrying in {wait_seconds}s ({attempt}/{attempts})")
            time.sleep(wait_seconds)


def delete_rss_missing_publications(master_sheet, project, rss_seen_by_channel, log_entries):
    delete_limit = int(project.get('rss_delete_limit', 5))
    if delete_limit <= 0:
        print("  ⏭️  RSS-missing deletion disabled for project")
        return

    recent_rows = get_recent_published_video_rows(
        master_sheet,
        project['name'],
        hours=config.RSS_FALLBACK_AGE_HOURS,
    )
    deleted = 0
    candidates = []

    for row in recent_rows:
        channel_seen = rss_seen_by_channel.get(row['channel_id'])
        if channel_seen is None or row['video_id'] in channel_seen:
            continue
        candidates.append(row)

    if len(candidates) > delete_limit:
        log_entries.append([
            format_timestamp(),
            project['name'],
            'RSS delete skipped',
            '',
            f'Candidates {len(candidates)} exceed project limit {delete_limit}',
            'skipped',
        ])
        print(f"  ⚠️  RSS-missing deletion skipped: {len(candidates)} candidates > limit {delete_limit}")
        return

    for row in candidates:
        if delete_telegram_message(project['bot_token'], project['channel_id'], row['message_id']):
            update_video_publication_status(
                master_sheet,
                row['video_id'],
                project['name'],
                status='deleted_rss_missing',
                error='RSS missing within recent window',
            )
            log_entries.append([
                format_timestamp(),
                project['name'],
                'Telegram post deleted',
                row['video_id'],
                'RSS missing within recent window',
                'deleted',
            ])
            deleted += 1

    if deleted:
        print(f"  🗑️  Deleted RSS-missing Telegram posts: {deleted}")


def load_project_channels(client, master_sheet, projects):
    project_channels = {}
    active_channels_dict = {}

    for project in projects:
        channels = load_youtube_channels(client, project)
        project_channels[project['name']] = channels

        project['channel_count'] = len(channels)
        for channel_id, channel_info in channels.items():
            if channel_id not in active_channels_dict:
                active_channels_dict[channel_id] = {
                    'channel_info': channel_info,
                    'projects': [],
                }
            active_channels_dict[channel_id]['projects'].append(project['name'])

    return project_channels, active_channels_dict


def split_project_names(value):
    return [item.strip() for item in str(value or '').split(',') if item.strip()]


def select_push_projects(master_sheet, projects, push_events):
    if not push_events:
        return []

    subscription_records = get_subscription_records(master_sheet)
    if subscription_records is None:
        print("  ⚠️  Could not read subscription project map; checking all projects")
        return projects

    target_project_names = set()
    missing_channels = []
    for event in push_events:
        record = subscription_records.get(event['channel_id'])
        if record:
            project_names = split_project_names(record.get('projects', ''))
            target_project_names.update(project_names)
            print(
                f"  🔎 Push map: {event['channel_id']} / {event['video_id']} -> "
                f"{', '.join(project_names) if project_names else '(no projects)'}"
            )
        else:
            missing_channels.append(event['channel_id'])

    if not target_project_names:
        print("  ⚠️  Push channels not found in subscription project map; checking all projects")
        return projects

    selected = [project for project in projects if project['name'] in target_project_names]
    missing = sorted(target_project_names - {project['name'] for project in selected})
    if missing:
        print(f"  ⚠️  Subscription map references missing projects: {', '.join(missing)}")
    if missing_channels:
        print(f"  ⚠️  Push channels missing from subscriptions: {', '.join(sorted(set(missing_channels)))}")
    print(f"  🎯 Push-only targets: {', '.join(project['name'] for project in selected)}")
    print(f"  🎯 Push-only target projects: {len(selected)} of {len(projects)}")
    return selected


def main():
    print("="*60)
    print("TOPUS - YouTube to Telegram Publisher")
    print("="*60)
    print(f"Started: {format_timestamp()}\n")
    
    master_sheet = None
    lock_acquired = False
    
    try:
        client = authenticate_google_sheets()
        try:
            master_sheet = open_master_sheet_with_retry(client)
        except Exception as error:
            if push_only_mode() and is_sheets_quota_error(error):
                print(f"\n⚠️  Google Sheets read quota is busy; push-only run will retry on the next dispatch: {error}")
                return
            raise

        if unlock_only_mode():
            print("  🔓 Unlock-only mode: clearing publisher lock")
            release_lock(master_sheet)
            return

        if maintenance_only_mode():
            print("  🧰 Maintenance-only mode: repairing workbook layout and values")
            maintain_workbook_layout(master_sheet)
            clean_known_workbook_text_values(master_sheet)
            update_last_run(master_sheet)
            update_run_status(master_sheet, 'complete: maintenance-only', run_status_details())
            return

        if repair_pending_only_mode():
            print("  🧩 Repair-pending-only mode: reconciling published rows")
            if not acquire_lock_with_wait(master_sheet):
                print("\n❌ Cannot acquire lock. Another process is running. Exiting.")
                update_run_status(master_sheet, 'busy: another run holds lock', run_status_details())
                return
            lock_acquired = True
            fixed = reconcile_pending_published_videos(master_sheet)
            deleted = delete_stale_unpublished_video_rows(master_sheet)
            ensure_non_settings_sheet_row_counts(master_sheet)
            update_run_status(master_sheet, f'complete: repaired pending rows={fixed}, deleted stale rows={deleted}', run_status_details())
            return

        if repair_video_id():
            print("  🧩 Repair-video mode: updating one known published row")
            if not acquire_lock_with_wait(master_sheet):
                print("\n❌ Cannot acquire lock. Another process is running. Exiting.")
                update_run_status(master_sheet, 'busy: another run holds lock', run_status_details())
                return
            lock_acquired = True
            ok = repair_video_publication_status(
                master_sheet,
                repair_video_id(),
                os.environ.get('TOPUS_REPAIR_PROJECT_NAME', '').strip(),
                os.environ.get('TOPUS_REPAIR_TG_MESSAGE_ID', '').strip(),
            )
            status = 'complete: repaired one video row' if ok else 'failed: repair video row not found'
            update_run_status(master_sheet, status, run_status_details())
            return
        
        # ПРОВЕРКА БЛОКИРОВКИ
        if not acquire_lock_with_wait(master_sheet):
            print("\n❌ Cannot acquire lock. Another process is running. Exiting.")
            update_run_status(master_sheet, 'busy: another run holds lock', run_status_details())
            return
        lock_acquired = True
        update_run_status(master_sheet, f'running: {run_mode_name()}', run_status_details())
        
        print("\n⚙️  Loading settings...")
        settings = load_settings(master_sheet)
        print_detection_latency_note()
        if push_only_mode():
            print("  ⚡ Push-only mode: skipping workbook maintenance")
        elif sync_only_mode():
            print("  📡 Sync-only mode: skipping workbook maintenance unrelated to subscriptions")
        else:
            print("  ⏭️  Publish mode: workbook maintenance is skipped")

        # Автоочистка старых записей
        if not push_only_mode() and not sync_only_mode():
            print("  ⏭️  Publish mode: old-record cleanup is skipped")
        
        print("\n📂 Loading projects...")
        projects = load_projects(master_sheet, update_status=not push_only_mode())

        push_events = []
        if push_only_mode():
            push_events = get_push_events(master_sheet)
            print(f"📬 Unprocessed push events: {len(push_events)}")
            if not push_events:
                print("\n✅ Push-only mode completed. No pending push events.")
                update_run_status(master_sheet, 'complete: no pending push events', run_status_details())
                return
            projects = select_push_projects(master_sheet, projects, push_events)
            if not projects:
                print("\n✅ Push-only mode completed. No target projects for pending push events.")
                update_run_status(master_sheet, 'complete: no target projects', run_status_details())
                return
        elif not sync_only_mode():
            print("  ⏭️  Video project link maintenance skipped during publish run")

        should_sync_subscriptions_now = (
            not push_only_mode()
            and (sync_only_mode() or should_force_subscription_sync())
        )

        if should_sync_subscriptions_now:
            deduplicate_subscription_rows(master_sheet)

        print("\n📺 Loading project channels...")
        project_channels, active_channels_dict = load_project_channels(client, master_sheet, projects)
        if push_only_mode():
            print("\n📡 Subscription sync skipped in push-only mode")
            subscription_sync_result = {'ok': True, 'partial': False, 'reason': ''}
        elif should_sync_subscriptions_now:
            subscription_sync_result = sync_subscriptions(
                client,
                master_sheet,
                projects,
                force=should_force_subscription_sync(),
                active_channels_dict=active_channels_dict,
            ) or {'ok': False, 'partial': True, 'reason': 'unknown subscription sync result'}
            update_project_channel_counts(master_sheet, projects)
        else:
            print("\n📡 Subscription sync skipped during publish run")
            subscription_sync_result = {'ok': True, 'partial': False, 'reason': ''}

        if sync_only_mode():
            print("\n✅ Sync-only mode completed. Skipping RSS/publish processing.")
            update_last_run(master_sheet)
            if subscription_sync_result.get('partial'):
                update_run_status(
                    master_sheet,
                    'partial: sync-only',
                    subscription_sync_result.get('reason') or run_status_details(),
                )
            else:
                update_run_status(master_sheet, 'complete: sync-only', run_status_details())
            return
        
        published_videos = get_published_videos(master_sheet)
        if not push_only_mode():
            push_events = get_push_events(master_sheet)
            print(f"📬 Unprocessed push events: {len(push_events)}")
        
        total_found = 0
        total_published = 0
        total_filtered = 0
        total_failed = 0
        
        # Аккумуляторы для батчевой записи
        videos_to_save = []
        log_entries = []
        rss_cache = {}
        video_info_cache = {}
        push_events_to_mark = {}
        publication_event_rows = {}

        def get_cached_video_info(video_id):
            if video_id not in video_info_cache:
                video_info_cache[video_id] = (
                    get_video_info_from_api(video_id),
                    get_last_youtube_api_error(),
                )
            return video_info_cache[video_id]

        def queue_push_event_mark(event, project_name):
            key = event['row_index']
            tracked = push_events_to_mark.setdefault(key, {
                'row_index': event['row_index'],
                'projects': event.get('projects', ''),
                'project_names': set(),
            })
            tracked['project_names'].add(project_name)
        
        for project in projects:
            print(f"\n{'='*60}")
            print(f"📁 Project: {project['name']}")
            print(f"{'='*60}")
            
            yt_channels = project_channels.get(project['name'], {})
            print(f"  📺 Active channels: {len(yt_channels)}")
            
            # Process push events
            if not project.get('push_api_enabled', True):
                print("  ⏭️  Push API disabled for project")
            else:
                for event in push_events:
                    if event['channel_id'] not in yt_channels:
                        continue
                
                    key = publication_key(event['video_id'], project)

                    if key in published_videos:
                        queue_push_event_mark(event, project['name'])
                        continue
                
                    total_found += 1
                
                    video_info_api, youtube_error = get_cached_video_info(event['video_id'])
                
                    if not video_info_api:
                        if youtube_error:
                            print(f"  ⚠️  YouTube API unavailable for {event['video_id']}: {youtube_error}")
                            log_entries.append([
                                format_timestamp(),
                                project['name'],
                                'YouTube API unavailable',
                                event['video_id'],
                                youtube_error,
                                'error',
                            ])
                            total_failed += 1
                            continue
                        queue_push_event_mark(event, project['name'])
                        continue
                
                    channel_info = yt_channels[event['channel_id']]
                
                    video = {
                        'video_id': event['video_id'],
                        'title': video_info_api['title'],
                        'url': f"https://www.youtube.com/watch?v={event['video_id']}",
                        'channel': video_info_api['channel'],
                        'channel_id': event['channel_id'],
                        'source_method': 'Push',
                    }
                    copy_video_classification(video, video_info_api)
                
                    video_published_date = video_info_api['published']
                    stale_reason = get_stale_reason(video_published_date, project)
                    if stale_reason:
                        print(f"  🚫 Skipped stale: {video['title'][:50]} ({stale_reason})")
                        timestamp = format_timestamp()
                        log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], stale_reason, 'filtered'])
                        queue_push_event_mark(event, project['name'])
                        published_videos.add(key)
                        total_filtered += 1
                        continue
                
                    should_filter, filter_reason = should_filter_video(video_info_api, project)
                    if should_filter:
                        print(f"  🚫 Filtered: {video['title'][:50]} ({filter_reason})")
                        timestamp = format_timestamp()
                        log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], filter_reason, 'filtered'])
                        videos_to_save.append((video, project, video_published_date, None, f"FILTERED: {filter_reason}"))
                        publication_event_rows[key] = event
                        published_videos.add(key)
                        total_filtered += 1
                        continue
                
                    # СНАЧАЛА добавляем в батч для сохранения
                    videos_to_save.append((video, project, video_published_date, None, None))
                    publication_event_rows[key] = event
                    published_videos.add(key)
                
                    print(f"  📝 Queued: {video['title'][:50]}...")
            
            if push_only_mode():
                print("  ⏭️  RSS fallback skipped in push-only mode")
                continue
            if not project.get('rss_feed_enabled', True):
                print("  ⏭️  RSS feed disabled for project")
                continue

            # RSS fallback check
            rss_videos, rss_seen_by_channel = rss_fallback_check(
                client,
                project,
                published_videos,
                project_channels=yt_channels,
                return_seen=True,
                rss_cache=rss_cache,
            )
            delete_rss_missing_publications(master_sheet, project, rss_seen_by_channel, log_entries)
            
            for video in rss_videos:
                video['source_method'] = 'RSS'
                channel_info = video['channel_info']
                key = publication_key(video['video_id'], project)
                
                total_found += 1
                
                video_info_api, _ = get_cached_video_info(video['video_id'])
                
                if video_info_api:
                    copy_video_classification(video, video_info_api)
                    video_published_date = video_info_api['published']
                    stale_reason = get_stale_reason(video_published_date, project)
                    if stale_reason:
                        print(f"    🚫 Skipped stale (RSS): {video['title'][:50]} ({stale_reason})")
                        timestamp = format_timestamp()
                        log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], f"RSS: {stale_reason}", 'filtered'])
                        published_videos.add(key)
                        total_filtered += 1
                        continue
                    
                    should_filter, filter_reason = should_filter_video(video_info_api, project)
                    
                    if should_filter:
                        print(f"    🚫 Filtered (RSS): {video['title'][:50]} ({filter_reason})")
                        timestamp = format_timestamp()
                        log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], f"RSS: {filter_reason}", 'filtered'])
                        videos_to_save.append((video, project, video_published_date, None, f"FILTERED: RSS: {filter_reason}"))
                        published_videos.add(key)
                        total_filtered += 1
                        continue
                else:
                    video_published_date = video.get('published', format_timestamp())
                    stale_reason = get_stale_reason(video_published_date, project)
                    if stale_reason:
                        print(f"    🚫 Skipped stale (RSS): {video['title'][:50]} ({stale_reason})")
                        timestamp = format_timestamp()
                        log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], f"RSS: {stale_reason}", 'filtered'])
                        published_videos.add(key)
                        total_filtered += 1
                        continue
                
                # СНАЧАЛА добавляем в батч для сохранения
                videos_to_save.append((video, project, video_published_date, None, None))
                published_videos.add(key)
                
                print(f"    📝 Queued (RSS): {video['title'][:50]}...")
        
        # СОХРАНЯЕМ ВСЕ ВИДЕО БАТЧАМИ
        print(f"\n💾 Saving {len(videos_to_save)} videos to table...")
        saved_publications = set(save_videos_batch(master_sheet, videos_to_save))
        print(f"  ✅ Saved {len(saved_publications)} new publication rows")
        for key in saved_publications:
            event = publication_event_rows.get(key)
            if event:
                queue_push_event_mark(event, key[1])

        if push_events_to_mark:
            print(f"  ✅ Marking processed push events: {len(push_events_to_mark)}")
            mark_push_events_processed_batch(master_sheet, push_events_to_mark.values())
        
        # ТЕПЕРЬ ПУБЛИКУЕМ В TELEGRAM
        print(f"\n📤 Publishing to Telegram...")
        
        for video, project, video_published_date, _, error in videos_to_save:
            if str(error or '').startswith('FILTERED: '):
                continue

            key = publication_key(video['video_id'], project)
            if key not in saved_publications:
                print(f"  ⏭️  Skipping {video['video_id']} / {project['name']} - already tracked or not saved")
                continue

            stale_reason = get_stale_reason(video_published_date, project)
            if stale_reason:
                print(f"  🚫 Skipping publish stale: {video['title'][:50]} ({stale_reason})")
                timestamp = format_timestamp()
                log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], stale_reason, 'filtered'])
                update_video_publication_status(master_sheet, video['video_id'], project['name'], status='filtered', error=stale_reason)
                total_filtered += 1
                continue
            
            channel_info = video.get('channel_info', {})
            
            template = channel_info.get('template') or project['default_template']
            message = format_message(template, video, channel_info, project)
            
            print(f"  📤 Publishing: {video['title'][:50]}...")
            
            tg_message_id = send_to_telegram(
                project['bot_token'],
                project['channel_id'],
                message
            )
            
            if tg_message_id:
                print(f"    ✅ Published (msg: {tg_message_id})")
                timestamp = format_timestamp()
                log_entries.append([timestamp, project['name'], 'Video published', video['video_id'], f"Telegram msg: {tg_message_id}", 'success'])
                update_video_publication_status(
                    master_sheet,
                    video['video_id'],
                    project['name'],
                    tg_message_id=tg_message_id,
                    status='published',
                    error=publication_status_detail(video),
                )
                total_published += 1
            else:
                print(f"    ❌ Failed to publish")
                timestamp = format_timestamp()
                log_entries.append([timestamp, project['name'], 'Publish failed', video['video_id'], 'Telegram error', 'error'])
                update_video_publication_status(master_sheet, video['video_id'], project['name'], status='failed', error='Telegram error')
                total_failed += 1
            
            time.sleep(1 / config.TELEGRAM_RATE_LIMIT)
        
        # СОХРАНЯЕМ ЛОГИ БАТЧЕМ
        if log_entries:
            print(f"\n📝 Saving logs...")
            log_events_batch(master_sheet, log_entries)
        
        # Обновление метаданных
        print("\n📝 Updating metadata...")
        if get_youtube_api_calls() > 0:
            update_youtube_quota(master_sheet, get_youtube_api_calls())
        update_last_run(master_sheet)
        
        # Final summary
        print(f"\n{'='*60}")
        print("📊 SUMMARY")
        print(f"{'='*60}")
        print(f"Videos found: {total_found}")
        print(f"  ✅ Published: {total_published}")
        print(f"  🚫 Filtered: {total_filtered}")
        print(f"  ❌ Failed: {total_failed}")
        print(f"  📊 YouTube API calls: {get_youtube_api_calls()}")
        print(f"\nFinished: {format_timestamp()}")
        print(f"{'='*60}")
        update_run_status(
            master_sheet,
            f'complete: found {total_found}, published {total_published}, filtered {total_filtered}, failed {total_failed}',
            run_status_details(),
        )
        
    except Exception as e:
        print(f"\n❌❌❌ FATAL ERROR: {e}")
        if master_sheet:
            update_run_status(master_sheet, f'failed: {type(e).__name__}', str(e)[:300])
        import traceback
        traceback.print_exc()
        raise
        
    finally:
        if master_sheet and lock_acquired:
            release_lock(master_sheet)


if __name__ == "__main__":
    main()
