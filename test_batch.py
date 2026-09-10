import io
import json
import unittest
from unittest.mock import patch
from listener import RPC, Collector, RpcError, RateLimited
from urllib.error import HTTPError


class BatchTests(unittest.TestCase):
    def test_rate_limit_does_not_fan_out_to_individual_requests(self):
        rpc = RPC('https://example.invalid', spacing=0)
        with patch('urllib.request.urlopen', side_effect=HTTPError('', 429, '', {}, None)), patch.object(rpc, 'parallel') as fallback:
            with self.assertRaises(RateLimited): rpc.many([('read', [1])])
            fallback.assert_not_called()
        self.assertFalse(getattr(rpc, 'batch_disabled', False))

    def test_large_watch_set_uses_one_filter_and_splits_on_rejection(self):
        rpc = unittest.mock.Mock()
        rpc.call.return_value = []
        c = Collector(None, rpc)
        addresses = [str(i) for i in range(647)]
        self.assertEqual(c.logs(addresses, 1, 10), [])
        self.assertEqual(rpc.call.call_count, 1)
        rpc.reset_mock()
        accepted = []
        def respond(method, params):
            group = params[0]['address']
            if len(group) > 100: raise RpcError('limit')
            accepted.extend(group)
            return []
        rpc.call.side_effect = respond
        self.assertEqual(c.logs(addresses, 1, 10), [])
        self.assertEqual(sorted(accepted), sorted(addresses))

    def test_parallel_reads_keep_order_and_propagate_failure(self):
        rpc = RPC('https://example.invalid', spacing=0)
        with patch.object(RPC, 'call', autospec=True, side_effect=lambda self, m, p: p[0]):
            self.assertEqual(rpc.parallel([('read', [i]) for i in range(20)]), list(range(20)))
        with patch.object(RPC, 'call', side_effect=RuntimeError('failed')):
            with self.assertRaises(RuntimeError): rpc.parallel([('read', [1])])

    def test_reordered_response_and_bounded_requests(self):
        sizes = []
        def respond(req, timeout):
            rows = json.loads(req.data)
            sizes.append(len(rows))
            return io.BytesIO(json.dumps([
                {'id': r['id'], 'result': r['params'][0]} for r in reversed(rows)
            ]).encode())
        with patch('urllib.request.urlopen', side_effect=respond):
            result = RPC('https://example.invalid', spacing=0).many([('read', [i]) for i in range(123)])
        self.assertEqual(result, list(range(123)))
        self.assertEqual(sizes, [10] * 12 + [3])

    def test_duplicate_ids_fall_back_without_using_bad_results(self):
        rpc = RPC('https://example.invalid', spacing=0)
        with patch('urllib.request.urlopen', return_value=io.BytesIO(b'[{"id":0,"result":99},{"id":0,"result":99}]')), patch.object(rpc, 'parallel', return_value=[1, 2]) as call:
            self.assertEqual(rpc.many([('read', [1]), ('read', [2])]), [1, 2])
            self.assertEqual(call.call_count, 1)
        self.assertTrue(rpc.batch_disabled)
