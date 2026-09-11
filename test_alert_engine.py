import json
import tempfile
import unittest
from pathlib import Path

from alert_engine import evaluate, matches, schema, source_wallets
from listener import database
from wallet_profiler import schema as wallet_schema


def candidate(**overrides):
    c=dict(id='0x'+'1'*40,protocol='pons_v2',activity_score=72,conviction_score=74,safety_score=65,safety_status='screened',safety_findings=[],buys=12,sells=3,unique_buyers=6,repeat_buyers=2,unique_senders=4,routed_share=.25,activity_acceleration=2,age_blocks=500,buy_sell_ratio=4)
    c.update(overrides);return c


class AlertTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.db=database(Path(self.tmp.name)/'x.sqlite');schema(self.db)
    def tearDown(self):self.db.close();self.tmp.cleanup()
    def test_trench_rule_requires_wallet_and_safety_evidence(self):
        self.assertEqual(matches(candidate())[0][0],'trench-candidate')
        self.assertNotIn('trench-candidate',[r[0] for r in matches(candidate(unique_senders=1))])
        self.assertNotIn('trench-candidate',[r[0] for r in matches(candidate(safety_status='incomplete'))])
    def test_deduplicates_active_alert_and_rearms(self):
        self.assertEqual(len(evaluate(self.db,[candidate()],cooldown=0)),1)
        self.assertEqual(len(evaluate(self.db,[candidate()],cooldown=0)),0)
        evaluate(self.db,[],cooldown=0)
        self.assertEqual(len(evaluate(self.db,[candidate()],cooldown=0)),1)
        self.assertEqual(self.db.execute('select count(*) from alerts').fetchone()[0],2)
    def test_critical_contract_risk(self):
        result=matches(candidate(safety_status='higher-risk',safety_score=10))
        self.assertEqual(result[0][:2],('contract-risk','critical'))
    def test_source_wallet_snapshot_is_traceable_and_does_not_invent_pnl(self):
        wallet_schema(self.db);asset=candidate()['id'];wallet='0x'+'2'*40
        with self.db:
            self.db.execute('INSERT INTO wallet_profiles VALUES(?,?,?,?,?,?,?,?,?)',(wallet,'x',3,1,2,1,100,120,72))
            for i,(name,values) in enumerate((('CurveBuy',{'buyer':wallet,'quoteIn':'100','tokensOut':'900'}),('CurveSell',{'seller':wallet,'quoteOut':'150','tokensIn':'400'}))):
                self.db.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',('0x'+str(i+1).zfill(64),i,110+i,'0x'+'0'*64,asset,'curve',name,asset,'2026-09-11T05:00:00+00:00',1000+i,json.dumps(values),'{}',None))
        item=source_wallets(self.db,asset)[0]
        self.assertEqual((item['quote_in_raw'],item['recorded_sells_since_buy'],item['quote_out_raw_since_buy']),('100',1,'150'))
        self.assertIsNone(item['realized_pnl_usd'])
    def test_smart_wallet_watch_allows_explicitly_unknown_audit(self):
        rules=[r[0] for r in matches(candidate(conviction_score=None,safety_score=None,safety_status='unknown',smart_wallets=5,activity_score=65,buys=25,sells=8))]
        self.assertIn('smart-wallet-watch',rules)

if __name__=='__main__':unittest.main()
