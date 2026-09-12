import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from listener import database
from market_normalizer import abi_text, calculate, indexed_pool, schema


class MarketNormalizerTests(unittest.TestCase):
    def test_decodes_dynamic_erc20_text(self):
        raw=(32).to_bytes(32,'big')+(3).to_bytes(32,'big')+b'JAR'+b'\0'*29
        self.assertEqual(abi_text('0x'+raw.hex()),'JAR')

    def test_trade_windows_price_and_volume(self):
        with tempfile.TemporaryDirectory() as d:
            db=database(Path(d)/'x.sqlite');schema(db)
            rows=[]
            for timestamp,quote,tokens in ((100,1000000,10*10**18),(3900,3000000,10*10**18),(3990,2000000,5*10**18)):
                rows.append({'event_timestamp':timestamp,'decoded':json.dumps({'quoteIn':str(quote),'tokensOut':str(tokens)})})
            result=calculate(rows,18,6,4000)
            self.assertAlmostEqual(result['price_quote'],.4)
            self.assertEqual(result['volume_5m_quote'],5)
            self.assertAlmostEqual(result['change_1h'],300)
            db.close()

    @patch('market_normalizer.request.urlopen')
    def test_indexed_pool_selects_deepest_matching_robinhood_pool(self,open_url):
        body={'pairs':[{'chainId':'ethereum','baseToken':{'address':'0xabc'},'quoteToken':{},'liquidity':{'usd':999}},
          {'chainId':'robinhood','baseToken':{'address':'0xabc'},'quoteToken':{},'liquidity':{'usd':10}},
          {'chainId':'robinhood','baseToken':{},'quoteToken':{'address':'0xAbC'},'liquidity':{'usd':50}}]}
        response=MagicMock();response.__enter__.return_value.read.return_value=json.dumps(body).encode()
        open_url.return_value=response
        self.assertEqual(indexed_pool('0xabc')['liquidity']['usd'],50)

if __name__=='__main__':unittest.main()
