import json
from pathlib import Path
import tempfile
import unittest

from listener import database, now
from market_normalizer import schema as market_schema
from wallet_profiler import rebuild, rebuild_pnl, schema


class WalletProfilerTests(unittest.TestCase):
    def test_observed_realized_pnl_uses_average_cost_and_quote_units(self):
        with tempfile.TemporaryDirectory() as d:
            db=database(Path(d)/'p.sqlite');schema(db);market_schema(db)
            asset='0x'+'a'*40;wallet='0x'+'1'*40
            with db:
                db.execute("INSERT INTO market_snapshots(asset,updated_at,decimals,quote_decimals,quote_symbol,source,status) VALUES(?,?,?,?,?,?,?)",(asset,now(),18,6,'NVDA','test','quote-only'))
                values=(('CurveBuy',{'buyer':wallet,'quoteIn':str(100*10**6),'tokensOut':str(10*10**18)}),('CurveSell',{'seller':wallet,'quoteOut':str(75*10**6),'tokensIn':str(5*10**18)}))
                for i,(name,decoded) in enumerate(values):
                    db.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',('0x'+str(i+1).zfill(64),i,100+i,'0x'+'0'*64,asset,'curve',name,asset,now(),100+i,json.dumps(decoded),'{}',None))
            self.assertEqual(rebuild_pnl(db),1)
            row=db.execute('SELECT position_tokens,cost_basis_quote,realized_pnl_quote FROM wallet_asset_pnl').fetchone()
            self.assertEqual(tuple(row),(5.0,50.0,25.0))
            perf=db.execute('SELECT win_rate,realized_by_quote,coverage FROM wallet_performance').fetchone()
            self.assertEqual((perf[0],json.loads(perf[1]),perf[2]),(100.0,{'NVDA':25.0},'listener-window'));db.close()

    def test_profiles_early_wallet_and_shared_sender_cluster(self):
        with tempfile.TemporaryDirectory() as d:
            db=database(Path(d)/'x.sqlite');schema(db)
            asset='0x'+'a'*40;sender='0x'+'f'*40
            with db:
                db.execute('INSERT INTO watches(address,kind,asset,created_block) VALUES(?,?,?,?)',(asset,'curve',asset,100))
                for i,w in enumerate(('0x'+'1'*40,'0x'+'2'*40)):
                    tx='0x'+str(i+1).zfill(64);decoded=json.dumps({'buyer':w})
                    db.execute(
                        'INSERT INTO events(tx_hash,log_index,block_number,block_hash,address,kind,name,asset,observed_at,event_timestamp,decoded,raw,decode_error) '
                        'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                        (tx,i,110+i,'0x'+'0'*64,asset,'curve','CurveBuy',asset,now(),110+i,decoded,'{}',None),
                    )
                    db.execute('INSERT INTO tx_attributions VALUES(?,?,?,?,?,?,?)',(tx,asset,sender,w,'routed',now(),None))
            self.assertEqual(rebuild(db),2)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM wallet_profiles WHERE early_assets=1').fetchone()[0],2)
            cluster=db.execute('SELECT members,transactions FROM wallet_clusters').fetchone();self.assertEqual(tuple(cluster),(2,2));db.close()

if __name__=='__main__':unittest.main()
