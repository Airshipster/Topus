import time
import os
from datetime import datetime

import config
from filters import should_filter_video
from rss import rss_fallback_check
from sheets import (
    acquire_lock,
    authenticate_google_sheets,
    cleanup_old_records,
    format_timestamp,
    get_recent_published_video_rows,
    get_published_videos,
    get_push_events,
    load_projects,
    load_settings,
    load_youtube_channels,
    log_events_batch,
    mark_push_event_processed,
    release_lock,
    save_videos_batch,
    update_project_runtime_status,
    update_video_publication_status,
    update_last_run,
    update_youtube_quota,
    parse_datetime_value,
)
from subscriptions import sync_subscriptions
from telegram_client import delete_telegram_message, format_message, send_to_telegram
from youtube_client import get_video_info_from_api, get_youtube_api_calls


def parse_datetime(value):
    return parse_datetime_value(value)


def get_stale_reason(published_at):
    published = parse_datetime(published_at)
    if not published:
        return ''

    age_hours = (datetime.utcnow() - published).total_seconds() / 3600
    if age_hours > config.MAX_PUBLISH_AGE_HOURS:
        return f"Stale video ({age_hours:.1f}h old, limit {config.MAX_PUBLISH_AGE_HOURS}h)"

    return ''


def should_force_subscription_sync():
    value = os.environ.get('TOPUS_FORCE_SUBSCRIPTION_SYNC', '')
    return value.lower() in ('1', 'true', 'yes')


def sync_only_mode():
    value = os.environ.get('TOPUS_SYNC_ONLY', '')
    return value.lower() in ('1', 'true', 'yes')


def push_only_mode():
    value = os.environ.get('TOPUS_PUSH_ONLY', '')
    return value.lower() in ('1', 'true', 'yes')


def publication_key(video_id, project):
    return (video_id, project['name'])


def delete_rss_missing_publications(master_sheet, project, rss_seen_by_channel, log_entries):
    recent_rows = get_recent_published_video_rows(
        master_sheet,
        project['name'],
        hours=config.RSS_FALLBACK_AGE_HOURS,
    )
    deleted = 0

    for row in recent_rows:
        channel_seen = rss_seen_by_channel.get(row['channel_id'])
        if channel_seen is None or row['video_id'] in channel_seen:
            continue

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

        if project.get('channels_error'):
            update_project_runtime_status(master_sheet, project, 'error', project['channels_error'])
        else:
            update_project_runtime_status(master_sheet, project, 'ready', '')

        for channel_id, channel_info in channels.items():
            if channel_id not in active_channels_dict:
                active_channels_dict[channel_id] = {
                    'channel_info': channel_info,
                    'projects': [],
                }
            active_channels_dict[channel_id]['projects'].append(project['name'])

    return project_channels, active_channels_dict


def main():
    print("="*60)
    print("TOPUS - YouTube to Telegram Publisher")
    print("="*60)
    print(f"Started: {format_timestamp()}\n")
    
    master_sheet = None
    
    try:
        client = authenticate_google_sheets()
        master_sheet = client.open_by_key(config.SPREADSHEET_ID)
        
        # ПРОВЕРКА БЛОКИРОВКИ
        if not acquire_lock(master_sheet):
            print("\n❌ Cannot acquire lock. Another process is running. Exiting.")
            return
        
        # Автоочистка старых записей
        cleanup_old_records(master_sheet)
        
        print("\n⚙️  Loading settings...")
        settings = load_settings(master_sheet)
        
        print("\n📂 Loading projects...")
        projects = load_projects(master_sheet)

        print("\n📺 Loading project channels...")
        project_channels, active_channels_dict = load_project_channels(client, master_sheet, projects)
        sync_subscriptions(
            client,
            master_sheet,
            projects,
            force=should_force_subscription_sync(),
            active_channels_dict=active_channels_dict,
        )

        if sync_only_mode():
            print("\n✅ Sync-only mode completed. Skipping RSS/publish processing.")
            update_last_run(master_sheet)
            return
        
        published_videos = get_published_videos(master_sheet)
        
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
        
        for project in projects:
            print(f"\n{'='*60}")
            print(f"📁 Project: {project['name']}")
            print(f"{'='*60}")
            
            yt_channels = project_channels.get(project['name'], {})
            print(f"  📺 Active channels: {len(yt_channels)}")
            
            # Process push events
            for event in push_events:
                if event['channel_id'] not in yt_channels:
                    continue
                
                key = publication_key(event['video_id'], project)

                if key in published_videos:
                    mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                    continue
                
                total_found += 1
                
                video_info_api = get_video_info_from_api(event['video_id'])
                
                if not video_info_api:
                    mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                    continue
                
                channel_info = yt_channels[event['channel_id']]
                
                video = {
                    'video_id': event['video_id'],
                    'title': video_info_api['title'],
                    'url': f"https://www.youtube.com/watch?v={event['video_id']}",
                    'channel': video_info_api['channel'],
                    'channel_id': event['channel_id']
                }
                
                video_published_date = video_info_api['published']
                stale_reason = get_stale_reason(video_published_date)
                if stale_reason:
                    print(f"  🚫 Skipped stale: {video['title'][:50]} ({stale_reason})")
                    timestamp = format_timestamp()
                    log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], stale_reason, 'filtered'])
                    videos_to_save.append((video, project, video_published_date, None, f"FILTERED: {stale_reason}"))
                    published_videos.add(key)
                    mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                    total_filtered += 1
                    continue
                
                should_filter, filter_reason = should_filter_video(video_info_api, project)
                if should_filter:
                    print(f"  🚫 Filtered: {video['title'][:50]} ({filter_reason})")
                    timestamp = format_timestamp()
                    log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], filter_reason, 'filtered'])
                    videos_to_save.append((video, project, video_published_date, None, f"FILTERED: {filter_reason}"))
                    published_videos.add(key)
                    mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                    total_filtered += 1
                    continue
                
                # СНАЧАЛА добавляем в батч для сохранения
                videos_to_save.append((video, project, video_published_date, None, None))
                published_videos.add(key)
                
                print(f"  📝 Queued: {video['title'][:50]}...")
                
                mark_push_event_processed(master_sheet, event['row_index'], project['name'])
            
            if push_only_mode():
                print("  ⏭️  RSS fallback skipped in push-only mode")
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
                channel_info = video['channel_info']
                key = publication_key(video['video_id'], project)
                
                total_found += 1
                
                video_info_api = get_video_info_from_api(video['video_id'])
                
                if video_info_api:
                    video_published_date = video_info_api['published']
                    stale_reason = get_stale_reason(video_published_date)
                    if stale_reason:
                        print(f"    🚫 Skipped stale (RSS): {video['title'][:50]} ({stale_reason})")
                        timestamp = format_timestamp()
                        log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], f"RSS: {stale_reason}", 'filtered'])
                        videos_to_save.append((video, project, video_published_date, None, f"FILTERED: RSS: {stale_reason}"))
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
                    stale_reason = get_stale_reason(video_published_date)
                    if stale_reason:
                        print(f"    🚫 Skipped stale (RSS): {video['title'][:50]} ({stale_reason})")
                        timestamp = format_timestamp()
                        log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], f"RSS: {stale_reason}", 'filtered'])
                        videos_to_save.append((video, project, video_published_date, None, f"FILTERED: RSS: {stale_reason}"))
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
        
        # ТЕПЕРЬ ПУБЛИКУЕМ В TELEGRAM
        print(f"\n📤 Publishing to Telegram...")
        
        for video, project, video_published_date, _, error in videos_to_save:
            if str(error or '').startswith('FILTERED: '):
                continue

            key = publication_key(video['video_id'], project)
            if key not in saved_publications:
                print(f"  ⏭️  Skipping {video['video_id']} / {project['name']} - already tracked or not saved")
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
                update_video_publication_status(master_sheet, video['video_id'], project['name'], tg_message_id=tg_message_id, status='published')
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
        
    except Exception as e:
        print(f"\n❌❌❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if master_sheet:
            release_lock(master_sheet)


if __name__ == "__main__":
    main()
