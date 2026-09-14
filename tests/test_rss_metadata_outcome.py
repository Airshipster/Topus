import ast
from pathlib import Path
import unittest


class RssMetadataOutcomeTests(unittest.TestCase):
    def outcome(self, error):
        # Execute the actual RSS missing-metadata branch without running publication.
        tree = ast.parse((Path(__file__).parents[1] / 'src/main.py').read_text(encoding='utf-8'))
        loop = next(n for n in ast.walk(tree) if isinstance(n, ast.For)
                    and isinstance(n.iter, ast.Name) and n.iter.id == 'rss_videos')
        branch = next(n for n in loop.body if isinstance(n, ast.If)
                      and ast.unparse(n.test) == 'not video_info_api')
        fixture = ast.parse('for _ in [0]:\n pass')
        fixture.body[0].body = [branch]
        state = {'video_info_api': None, 'api_error': error, 'total_failed': 0,
                 'video': {'video_id': 'test'}, 'print': lambda *args: None}
        exec(compile(ast.fix_missing_locations(fixture), '<rss-branch>', 'exec'), state)
        return state['total_failed']

    def test_unavailable_video_does_not_fail_scan(self):
        self.assertEqual(self.outcome(None), 0)

    def test_real_api_failure_remains_failure(self):
        self.assertEqual(self.outcome('HTTP 503'), 1)
