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
)
from subscriptions import sync_subscriptions
from telegram_client import format_message, send_to_telegram
from youtube_client import get_video_info_from_api, get_youtube_api_calls


def parse_datetime(value):
    if not value:
        return None

    try:
        if value.endswith('Z'):
            return datetime.fromisoformat(value.replace('Z', '+00:00')).replace(tzinfo=None)

        return datetime.fromisoformat(value).replace(tzinfo=None)
    except Exception:
        return None


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


def main():
    print("="*60)
    print("TOPUS - YouTube to Telegram Publisher")
    print("="*60)
    print(f"Started: {datetime.utcnow().isoformat()}Z\n")
    
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
        
        sync_subscriptions(client, master_sheet, projects, force=should_force_subscription_sync())
        
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
        
        for project in projects:
            print(f"\n{'='*60}")
            print(f"📁 Project: {project['name']}")
            print(f"{'='*60}")
            
            yt_channels = load_youtube_channels(client, project)
            if project.get('channels_error'):
                update_project_runtime_status(master_sheet, project, 'error', project['channels_error'])
            else:
                update_project_runtime_status(master_sheet, project, 'ready', '')
            print(f"  📺 Active channels: {len(yt_channels)}")
            
            # Process push events
            for event in push_events:
                if event['channel_id'] not in yt_channels:
                    continue
                
                if event['video_id'] in published_videos:
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
                    timestamp = datetime.utcnow().isoformat()
                    log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], stale_reason, 'filtered'])
                    videos_to_save.append((video, project, video_published_date, None, f"FILTERED: {stale_reason}"))
                    published_videos.add(video['video_id'])
                    mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                    total_filtered += 1
                    continue
                
                should_filter, filter_reason = should_filter_video(video_info_api, project)
                if should_filter:
                    print(f"  🚫 Filtered: {video['title'][:50]} ({filter_reason})")
                    timestamp = datetime.utcnow().isoformat()
                    log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], filter_reason, 'filtered'])
                    videos_to_save.append((video, project, video_published_date, None, f"FILTERED: {filter_reason}"))
                    published_videos.add(video['video_id'])
                    mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                    total_filtered += 1
                    continue
                
                # СНАЧАЛА добавляем в батч для сохранения
                videos_to_save.append((video, project, video_published_date, None, None))
                published_videos.add(video['video_id'])
                
                print(f"  📝 Queued: {video['title'][:50]}...")
                
                mark_push_event_processed(master_sheet, event['row_index'], project['name'])
            
            # RSS fallback check
            rss_videos = rss_fallback_check(client, project, published_videos)
            
            for video in rss_videos:
                channel_info = video['channel_info']
                
                total_found += 1
                
                video_info_api = get_video_info_from_api(video['video_id'])
                
                if video_info_api:
                    video_published_date = video_info_api['published']
                    stale_reason = get_stale_reason(video_published_date)
                    if stale_reason:
                        print(f"    🚫 Skipped stale (RSS): {video['title'][:50]} ({stale_reason})")
                        timestamp = datetime.utcnow().isoformat()
                        log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], f"RSS: {stale_reason}", 'filtered'])
                        videos_to_save.append((video, project, video_published_date, None, f"FILTERED: RSS: {stale_reason}"))
                        published_videos.add(video['video_id'])
                        total_filtered += 1
                        continue
                    
                    should_filter, filter_reason = should_filter_video(video_info_api, project)
                    
                    if should_filter:
                        print(f"    🚫 Filtered (RSS): {video['title'][:50]} ({filter_reason})")
                        timestamp = datetime.utcnow().isoformat()
                        log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], f"RSS: {filter_reason}", 'filtered'])
                        videos_to_save.append((video, project, video_published_date, None, f"FILTERED: RSS: {filter_reason}"))
                        published_videos.add(video['video_id'])
                        total_filtered += 1
                        continue
                else:
                    video_published_date = video.get('published', datetime.utcnow().isoformat())
                    stale_reason = get_stale_reason(video_published_date)
                    if stale_reason:
                        print(f"    🚫 Skipped stale (RSS): {video['title'][:50]} ({stale_reason})")
                        timestamp = datetime.utcnow().isoformat()
                        log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], f"RSS: {stale_reason}", 'filtered'])
                        videos_to_save.append((video, project, video_published_date, None, f"FILTERED: RSS: {stale_reason}"))
                        published_videos.add(video['video_id'])
                        total_filtered += 1
                        continue
                
                # СНАЧАЛА добавляем в батч для сохранения
                videos_to_save.append((video, project, video_published_date, None, None))
                published_videos.add(video['video_id'])
                
                print(f"    📝 Queued (RSS): {video['title'][:50]}...")
        
        # СОХРАНЯЕМ ВСЕ ВИДЕО БАТЧАМИ
        print(f"\n💾 Saving {len(videos_to_save)} videos to table...")
        saved_video_ids = save_videos_batch(master_sheet, videos_to_save)
        print(f"  ✅ Saved {len(saved_video_ids)} videos")
        
        # ТЕПЕРЬ ПУБЛИКУЕМ В TELEGRAM
        print(f"\n📤 Publishing to Telegram...")
        
        for video, project, video_published_date, _, error in videos_to_save:
            if str(error or '').startswith('FILTERED: '):
                continue

            if video['video_id'] not in saved_video_ids:
                print(f"  ⚠️  Skipping {video['video_id']} - not saved")
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
                timestamp = datetime.utcnow().isoformat()
                log_entries.append([timestamp, project['name'], 'Video published', video['video_id'], f"Telegram msg: {tg_message_id}", 'success'])
                update_video_publication_status(master_sheet, video['video_id'], project['name'], tg_message_id=tg_message_id, status='published')
                total_published += 1
            else:
                print(f"    ❌ Failed to publish")
                timestamp = datetime.utcnow().isoformat()
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
        print(f"\nFinished: {datetime.utcnow().isoformat()}Z")
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
