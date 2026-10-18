import os
import sys
import tempfile
import sqlite3
import json
import unittest
from datetime import datetime,timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
import sheets
import activity_retention
from publication_policy import publication_age_limit


class RecoveryPolicyTests(unittest.TestCase):
    def test_week_is_scoped_to_scitopus(self):
        with patch('config.MAX_PUBLISH_AGE_HOURS',24):
            self.assertEqual(publication_age_limit({'name':'SciTopus','max_publish_age_hours':168}),168)
            self.assertEqual(publication_age_limit({'name':'SciTopus','max_publish_age_hours':500}),168)
            self.assertEqual(publication_age_limit({'name':'Other','max_publish_age_hours':168}),24)
            self.assertEqual(publication_age_limit({'name':'Other','max_publish_age_hours':12}),12)

    def test_backup_status_has_same_semantics(self):
        for prefix in ['RSS','RSS [YouTube API backup]','Bot: RSS [YouTube API backup]']:
            for status in ['pending','published','filtered','expired','unavailable']:
                self.assertEqual(sheets.status_name_from_text(prefix+': '+status+'. details'),status)

    def test_scitopus_manual_replay_cannot_bypass_week(self):
        from main import get_stale_reason
        with patch.dict(os.environ,{'TOPUS_MAX_PUBLISH_AGE_HOURS_OVERRIDE':''}),\
             patch('main.current_local_datetime',return_value=datetime(2026,10,18,10)):
            self.assertTrue(get_stale_reason('10.10.2026 09:00:00',{'name':'SciTopus'},
                                            {'manual_replay':True}))
            self.assertFalse(get_stale_reason('13.10.2026 09:00:00',{'name':'SciTopus'}))
        with patch.dict(os.environ,{'TOPUS_MAX_PUBLISH_AGE_HOURS_OVERRIDE':'500'}),\
             patch('main.current_local_datetime',return_value=datetime(2026,10,18,10)):
            self.assertTrue(get_stale_reason('10.10.2026 09:00:00',{'name':'SciTopus'}))

    def test_publication_results_share_reads_and_keep_individual_send_times(self):
        headers=sheets.VIDEO_HEADERS
        first=['SciTopus','Channel','https://youtube.com/channel/channel',
               'One','https://youtube.com/watch?v=abcdefghijk',
               '17.10.2026 09:00:00','18.10.2026 09:00:00','','','','Push: pending']
        second=list(first)
        second[3:5]=['Two','https://youtube.com/watch?v=bcdefghijkl']
        second[-1]='Bot: RSS [YouTube API backup]: pending'
        values=[headers,first,second]
        formulas=[['Проект'],['=HYPERLINK("https://t.me/SciTopus";"SciTopus")'],['SciTopus']]
        entries=[{'video_id':'abcdefghijk','project_name':'SciTopus','tg_message_id':24907,
                  'timestamp':'18.10.2026 10:00:00'},
                 {'video_id':'bcdefghijkl','project_name':'SciTopus','mark_tg_published':True,
                  'timestamp':'18.10.2026 10:01:00'}]
        with patch.object(sheets,'ensure_videos_worksheet',return_value=SimpleNamespace()),\
             patch.object(sheets,'get_values_with_quota_retry',side_effect=[values,formulas]) as read,\
             patch.object(sheets,'batch_update_with_quota_retry') as write:
            count=sheets.update_video_publication_status_batch(None,entries)
        self.assertEqual(count,2)
        self.assertEqual(read.call_count,2)
        updates={item['range']:item['values'][0][0]
                 for call in write.call_args_list for item in call.args[1]}
        self.assertEqual(updates['J2'],24907)
        self.assertIn('SciTopus/24907',updates['A2'])
        self.assertEqual(updates['I2'],sheets.sheet_datetime_value(entries[0]['timestamp']))
        self.assertEqual(updates['I3'],sheets.sheet_datetime_value(entries[1]['timestamp']))
        self.assertIn('RSS [YouTube API backup]: published',updates['K3'])

    def test_old_notification_pending_expires_without_resending(self):
        old=['SciTopus','Channel','https://youtube.com/channel/channel','One',
             'https://youtube.com/watch?v=abcdefghijk','01.09.2026 09:00:00',
             '01.09.2026 09:00:00','','','','Bot: Push: notification-pending']
        with patch.object(sheets,'ensure_videos_worksheet',return_value=SimpleNamespace()),\
             patch.object(sheets,'get_values_with_quota_retry',return_value=[sheets.VIDEO_HEADERS,old]),\
             patch.object(sheets,'current_local_datetime',return_value=datetime(2026,10,18,10)),\
             patch.object(sheets,'batch_update_with_quota_retry') as write:
            count=sheets.delete_stale_unpublished_video_rows(None)
        self.assertEqual(count,1)
        text=write.call_args.args[1][0]['values'][0][0]
        self.assertIn('Stale video',text)
        self.assertNotIn('Duplicate',text)

    def test_terminal_rows_expire_but_unfinished_work_survives(self):
        now=datetime(2026,10,18,10)
        videos=SimpleNamespace(title='Глобальные видео',id=1,row_count=20000)
        logs=SimpleNamespace(title='Логи',id=2,row_count=20000)
        push=SimpleNamespace(title='Push события',id=3,row_count=20000)
        mapping={w.title:w for w in [videos,logs,push]}
        old='01.09.2026 12:00:00'
        data={videos.title:[['Системный статус','Дата обработки GMT+4'],
            ['RSS [YouTube API backup]: published',old],['RSS: expired',old],
            ['Push: unavailable',old],['Push: pending',old],
            ['Push: filtered','18.10.2026 09:00:00']],
            logs.title:[['Timestamp GMT+4'],[old]],
            push.title:[['Timestamp GMT+4','Обработано'],[old,'✅'],[old,'❌']]}
        deleted={}
        for worksheet in mapping.values():
            def selected(ranges,w=worksheet):
                result=[]
                for address in ranges:
                    col=ord(address[0])-ord('A')
                    result.append([[r[col]] if len(r)>col else [] for r in data[w.title][1:]])
                return result
            worksheet.batch_get=selected
        def remove(ss,w,rows): deleted[w.title]=list(rows);return len(rows)
        with patch.object(sheets,'get_values_with_quota_retry',side_effect=lambda w,*args:data[w.title][:1]),\
             patch.object(sheets,'current_local_datetime',return_value=now),\
             patch.object(sheets,'delete_rows_batch',side_effect=remove),\
             patch.object(sheets,'ensure_non_settings_sheet_row_counts'):
            total=sheets.delete_old_activity_rows(SimpleNamespace(worksheet=lambda name:mapping[name]))
        self.assertEqual(total,5)
        self.assertEqual(deleted[videos.title],[2,3,4])
        self.assertEqual(deleted[push.title],[2])

    def test_six_hour_retention_is_not_idle_polling(self):
        with tempfile.TemporaryDirectory() as root,patch.dict(os.environ,{'TOPUS_ACTIVITY_RETENTION_MARKER':root+'/ok'}):
            self.assertTrue(activity_retention.retention_due())
            activity_retention.mark_retention_completed()
            mtime=Path(root+'/ok').stat().st_mtime
            self.assertFalse(activity_retention.retention_due(mtime+60))
            self.assertTrue(activity_retention.retention_due(mtime+21601))

    def test_scitopus_callbacks_precede_old_backup_backlog(self):
        from push_store import prioritized_pending_events
        db=sqlite3.connect(':memory:');db.row_factory=sqlite3.Row
        db.executescript('CREATE TABLE events(video_id,channel_id,received,mirrored,source);'
                         'CREATE TABLE inventory_cache(id,payload);')
        db.execute('INSERT INTO inventory_cache VALUES (1,?)',
                   (json.dumps({'science':{'projects':['SciTopus']}}),))
        db.executemany('INSERT INTO events VALUES (?,?,?,?,?)',[
            ('other','other',1,0,'Push · server'),('backup','science',2,0,'YouTube API backup'),
            ('fresh','science',3,0,'Push · server')])
        self.assertEqual([r['video_id'] for r in prioritized_pending_events(db)],['fresh','backup','other'])
        db.close()

    def test_deferred_closure_is_batched_and_cannot_overwrite_sent(self):
        values=[['Проект','Ссылка на видео','TG message_id','Системный статус'],
                ['SciTopus','youtube.com/watch?v=abcdefghijk','','RSS: pending'],
                ['SciTopus','youtube.com/watch?v=bcdefghijkl','123','Push: published']]
        ws=SimpleNamespace()
        with patch.object(sheets,'ensure_videos_worksheet',return_value=ws),\
             patch.object(sheets,'get_values_with_quota_retry',return_value=values) as read,\
             patch.object(sheets,'batch_update_with_quota_retry') as write:
            count=sheets.close_deferred_videos_batch(None,[('abcdefghijk','SciTopus','expired','old'),
                ('bcdefghijkl','SciTopus','expired','old')])
        self.assertEqual(count,1)
        self.assertEqual(read.call_count,1)
        self.assertEqual(write.call_count,1)


if __name__=='__main__': unittest.main()
