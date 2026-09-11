import json
import tempfile
import unittest
from pathlib import Path

from alert_engine import evaluate, matches, schema, source_wallets, track_lifecycle, telegram_text
from listener import database
from wallet_profiler import schema as wallet_schema


def candidate(**overrides):
    c=dict(id='0x'+'1'*40,protocol='pons_v2',symbol='TEST',name='Test Token',market_status='quote-only',deployer='0x'+'d'*40,activity_score=72,conviction_score=74,safety_score=65,safety_status='screened',safety_findings=[],buys=12,sells=3,buys_5m=3,sells_5m=0,last_trade_age_seconds=30,dev_exit_detected=False,unique_buyers=6,repeat_buyers=2,unique_senders=4,routed_share=.25,activity_acceleration=2,age_blocks=500,buy_sell_ratio=4)
    c.update(overrides);return c


class AlertTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.db=database(Path(self.tmp.name)/'x.sqlite');schema(self.db)
    def tearDown(self):self.db.close();self.tmp.cleanup()
    def test_trench_rule_requires_wallet_and_safety_evidence(self):
        self.assertEqual(matches(candidate())[0][0],'trench-candidate')
        self.assertNotIn('trench-candidate',[r[0] for r in matches(candidate(unique_senders=1))])
        self.assertNotIn('trench-candidate',[r[0] for r in matches(candidate(safety_status='incomplete'))])
    def test_deduplicates_alert_permanently_even_after_rule_rearms(self):
        self.assertEqual(len(evaluate(self.db,[candidate()],cooldown=0)),1)
        self.assertEqual(len(evaluate(self.db,[candidate()],cooldown=0)),0)
        evaluate(self.db,[],cooldown=0)
        self.assertEqual(len(evaluate(self.db,[candidate()],cooldown=0)),0)
        self.assertEqual(self.db.execute('select count(*) from alerts').fetchone()[0],1)

    def test_positive_alpha_is_blocked_after_deployer_sell_or_stale_flow(self):
        self.assertFalse(any(r[0] != 'dev-exit' for r in matches(candidate(dev_exit_detected=True,dev_sell_count=1))))
        self.assertEqual(matches(candidate(buys_5m=0)),[])
        self.assertEqual(matches(candidate(deployer=None)),[])
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
    def test_unnamed_activity_cannot_be_called_smart_wallet_flow(self):
        rules=[r[0] for r in matches(candidate(conviction_score=None,safety_score=None,safety_status='unknown',smart_wallets=5,activity_score=65,buys=25,sells=8,symbol=None,name=None,market_status='unknown'))]
        self.assertFalse(any(r.startswith('smart-') for r in rules))
    def test_telegram_message_contains_traceable_evidence(self):
        row={'asset':candidate()['id'],'severity':'high','title':'Consensus','score':80,'evidence':json.dumps({'symbol':'JAR','name':'Jar Agent','quote_symbol':'WETH','quote_decimals':18,'buys':9,'sells':2,'source_wallets':[{'wallet':'0x'+'2'*40,'smart_score':70,'buy_count':3,'quote_in_raw':'1000000000000000000','buy_tx_url':'https://example/tx'}]})}
        message=telegram_text(row)
        self.assertIn('$JAR',message);self.assertIn('Jar Agent',message);self.assertIn('1 WETH',message);self.assertIn('repeat 3×',message);self.assertIn('https://example/tx',message)
        self.assertIn('<code>'+candidate()['id']+'</code>',message);self.assertIn('https://gmgn.ai/robinhood/token/'+candidate()['id'],message)

    def test_capital_rotation_requires_identified_safe_asset(self):
        good=candidate(symbol='MOVE',name='Move',market_status='quote-only',qualified_migrating_wallets_5m=2,migration_sources=1,age_blocks=500,buys_5m=2)
        self.assertIn('capital-rotation',[r[0] for r in matches(good)])
        good['symbol']=good['name']=None;good['market_status']='unknown'
        self.assertNotIn('capital-rotation',[r[0] for r in matches(good)])

    def test_lifecycle_tracks_returns_and_wallet_exits(self):
        with self.db:
            self.db.execute('CREATE TABLE market_snapshots(asset TEXT PRIMARY KEY,price_quote REAL)')
            self.db.execute('INSERT INTO market_snapshots VALUES(?,?)',(candidate()['id'],2.0))
            self.db.execute('INSERT INTO alerts(created_at,asset,rule,severity,title,score,evidence) VALUES(?,?,?,?,?,?,?)',('2026-09-10T00:00:00+00:00',candidate()['id'],'x','high','x',80,json.dumps({'source_wallets':[]})))
        self.assertEqual(track_lifecycle(self.db),1)
        row=self.db.execute('SELECT * FROM alert_lifecycle').fetchone()
        self.assertEqual((row['entry_price_quote'],row['current_return'],row['max_return']),(2.0,0.0,0.0))
        with self.db:self.db.execute('UPDATE market_snapshots SET price_quote=3')
        track_lifecycle(self.db);row=self.db.execute('SELECT * FROM alert_lifecycle').fetchone()
        self.assertEqual((row['current_return'],row['max_return'],row['drawdown_from_ath']),(50.0,50.0,0.0))

    def test_consensus_requires_profitable_and_independent_wallets(self):
        good=candidate(profitable_wallets_5m=1,profitable_wallets_15m=2,profitable_wallets_30m=3,independent_profitable_wallets_30m=2,symbol='REAL',name='Real',market_status='quote-only')
        self.assertIn('smart-money-consensus',[r[0] for r in matches(good)])
        good['independent_profitable_wallets_30m']=1
        self.assertNotIn('smart-money-consensus',[r[0] for r in matches(good)])

if __name__=='__main__':unittest.main()
