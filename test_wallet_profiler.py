import json
from pathlib import Path
import tempfile
import unittest

from listener import database, now
from wallet_profiler import rebuild, schema


class WalletProfilerTests(unittest.TestCase):
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
