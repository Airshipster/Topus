import pathlib
import sys
import unittest


SRC_DIR = pathlib.Path(__file__).resolve().parents[1] / 'src'
sys.path.insert(0, str(SRC_DIR))

from filters import contains_cyrillic_letter, should_filter_video


class CyrillicTitleFilterTests(unittest.TestCase):
    def setUp(self):
        self.project = {
            'allow_premieres': False,
            'allow_shorts': False,
            'allow_streams': False,
            'category_stop_words': {},
            'stop_words': [],
        }

    def test_detects_cyrillic_letters(self):
        self.assertTrue(contains_cyrillic_letter('Python для физиков'))
        self.assertTrue(contains_cyrillic_letter('Українська наука'))
        self.assertFalse(contains_cyrillic_letter('Deep Learning Tutorial 2026'))
        self.assertFalse(contains_cyrillic_letter('2026: AI + ML'))

    def test_allows_mixed_or_cyrillic_title(self):
        filtered, reason = should_filter_video({'title': 'Python для физиков'}, self.project)
        self.assertFalse(filtered)
        self.assertEqual('', reason)

    def test_filters_title_without_cyrillic(self):
        for title in ('Deep Learning Tutorial', '2026: AI + ML', '🚀 AI breakthrough'):
            with self.subTest(title=title):
                filtered, reason = should_filter_video({'title': title}, self.project)
                self.assertTrue(filtered)
                self.assertEqual('Title has no Cyrillic letters', reason)

    def test_filters_shorts_even_when_project_flag_allows_them(self):
        project = {**self.project, 'allow_shorts': True}
        filtered, reason = should_filter_video(
            {'title': 'Научный короткий ролик', 'is_short': True, 'duration_seconds': 125},
            project,
        )
        self.assertTrue(filtered)
        self.assertEqual('Short video (125s)', reason)


if __name__ == '__main__':
    unittest.main()
