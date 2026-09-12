import tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from listener import database
from social_enricher import schema,extract,safe_url,normalize,analyze
from test_listener import addr
class SocialTests(unittest.TestCase):
 def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.db=database(Path(self.tmp.name)/'s.sqlite');schema(self.db)
 def tearDown(self):self.db.close();self.tmp.cleanup()
 def test_rejects_local_urls_and_normalizes_identity(self):
  self.assertIsNone(safe_url('http://127.0.0.1/admin'));self.assertEqual(normalize('https://WWW.Example.com/X/'),'example.com/x')
 def test_extracts_dexscreener_socials(self):
  sites,socials=extract({'info':{'websites':[{'url':'https://itembase.co'}],'socials':[{'type':'twitter','url':'https://x.com/itembaseco'}]}})
  self.assertEqual(sites,['https://itembase.co']);self.assertEqual(socials['twitter'],'https://x.com/itembaseco')
 @patch('social_enricher.fetch_links',return_value=['https://x.com/itembaseco'])
 @patch('social_enricher.fetch_json')
 def test_cross_link_requires_project_page_reference(self,j,l):
  j.return_value={'pairs':[{'chainId':'robinhood','pairAddress':addr(2),'liquidity':{'usd':10},'info':{'websites':[{'url':'https://itembase.co'}],'socials':[{'type':'twitter','url':'https://x.com/itembaseco'}]}}]}
  analyze(self.db,addr(1));row=self.db.execute('SELECT * FROM social_identity').fetchone();self.assertEqual((row['cross_linked'],row['confidence'],row['status']),(1,'high','cross-linked'));self.assertGreaterEqual(row['score'],75)
if __name__=='__main__':unittest.main()
