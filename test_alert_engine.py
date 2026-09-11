import tempfile
import unittest
from pathlib import Path

from alert_engine import evaluate, matches, schema
from listener import database


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

if __name__=='__main__':unittest.main()
