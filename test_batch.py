import io
import json
import unittest
from unittest.mock import patch
from listener import RPC


class BatchTests(unittest.TestCase):
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
        self.assertEqual(sizes, [50, 50, 23])

    def test_duplicate_ids_fall_back_without_using_bad_results(self):
        rpc = RPC('https://example.invalid', spacing=0)
        with patch('urllib.request.urlopen', return_value=io.BytesIO(b'[{"id":0,"result":99},{"id":0,"result":99}]')), patch.object(rpc, 'call', side_effect=[1, 2]) as call:
            self.assertEqual(rpc.many([('read', [1]), ('read', [2])]), [1, 2])
            self.assertEqual(call.call_count, 2)
        self.assertTrue(rpc.batch_disabled)
