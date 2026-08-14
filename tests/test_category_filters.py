import os
import sys
import unittest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from filters import should_filter_video  # noqa: E402


class CategoryFilterTests(unittest.TestCase):
    def setUp(self):
        self.project = {
            'allow_shorts': True,
            'allow_streams': True,
            'allow_premieres': True,
            'stop_words': [],
            'category_stop_words': {
                'научные институты и курсы': ['класс', 'егэ', 'огэ', 'введение'],
            },
        }

    def test_filters_whole_word_in_target_category(self):
        filtered, reason = should_filter_video(
            {'title': 'Физика, 8 класс'},
            self.project,
            {'category': 'Научные институты и курсы'},
        )
        self.assertTrue(filtered)
        self.assertIn('Category stop word', reason)

    def test_does_not_filter_same_word_outside_target_category(self):
        filtered, _ = should_filter_video(
            {'title': 'Физика, 8 класс'},
            self.project,
            {'category': 'Физика'},
        )
        self.assertFalse(filtered)

    def test_does_not_match_word_fragment(self):
        filtered, _ = should_filter_video(
            {'title': 'Классическая механика'},
            self.project,
            {'category': 'Научные институты и курсы'},
        )
        self.assertFalse(filtered)

    def test_filters_each_added_word_as_a_whole_word(self):
        for word in ('ЕГЭ', 'ОГЭ', 'введение'):
            with self.subTest(word=word):
                filtered, _ = should_filter_video(
                    {'title': f'Курс: {word} в физике'},
                    self.project,
                    {'category': 'Научные институты и курсы'},
                )
                self.assertTrue(filtered)

    def test_does_not_match_added_word_fragments(self):
        for title in ('введениевкурс', 'огэшник', 'егэшный разбор'):
            with self.subTest(title=title):
                filtered, _ = should_filter_video(
                    {'title': title},
                    self.project,
                    {'category': 'Научные институты и курсы'},
                )
                self.assertFalse(filtered)


if __name__ == '__main__':
    unittest.main()
