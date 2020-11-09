import unittest
import mock
from redis import ResponseError

from brainzutils import cache
from brainzutils import metrics


class MetricsTestCase(unittest.TestCase):

    def setUp(self):
        cache.init('redis')

    def tearDown(self):
        metrics._metrics_site_name = None

    @mock.patch('brainzutils.metrics.cache.hincrby')
    def test_increment(self, hincrby):
        metrics.init('listenbrainz.org')
        metrics.increment('test_m', 2)
        hincrby.assert_called_with('listenbrainz.org', 'test_m', 2, namespace='metrics')

    @mock.patch('brainzutils.metrics.cache.hincrby')
    def test_increment_default(self, hincrby):
        metrics.init('listenbrainz.org')
        metrics.increment('test_m')
        hincrby.assert_called_with('listenbrainz.org', 'test_m', 1, namespace='metrics')

    def test_increment_negative(self):
        metrics.init('listenbrainz.org')
        with self.assertRaises(ValueError):
            metrics.increment('test_m', -2)

    def test_increment_badname(self):
        metrics.init('listenbrainz.org')
        with self.assertRaises(ValueError):
            metrics.increment('tag')

    def test_increment_noinit(self):
        with self.assertRaises(RuntimeError):
            metrics.increment('test_m')

    @mock.patch('brainzutils.metrics.cache.hincrby')
    @mock.patch('brainzutils.metrics.cache.hset')
    def test_increment_overflow(self, hset, hincrby):
        hincrby.side_effect = [ResponseError("increment or decrement would overflow"), 10]
        metrics.init('listenbrainz.org')
        metrics.increment('test_m', 10)

        hincrby.assert_has_calls([mock.call('listenbrainz.org', 'test_m', 10, namespace='metrics'),
                                  mock.call('listenbrainz.org', 'test_m', 10, namespace='metrics')])
        hset.assert_called_with('listenbrainz.org', 'test_m', 0, namespace='metrics')

    @mock.patch('brainzutils.metrics.cache.hdel')
    def test_remove(self, hdel):
        metrics.init('listenbrainz.org')
        metrics.remove('test_m')
        hdel.assert_called_with('listenbrainz.org', ['test_m'], namespace='metrics')

    @mock.patch('brainzutils.metrics.cache.hgetall')
    def test_stats(self, hgetall):
        metrics.init('listenbrainz.org')
        hgetall.return_value = {b'valueone': b'1',
                                b'valuetwo': b'20',
                                b'somethingelse': b'8'}

        stats = metrics.stats()
        hgetall.assert_called_with('listenbrainz.org', namespace='metrics')

        expected = {'valueone': 1,
                    'valuetwo': 20,
                    'somethingelse': 8,
                    'tag': 'listenbrainz.org'}

        self.assertEqual(stats, expected)
