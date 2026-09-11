import json
from pathlib import Path
import tempfile
import unittest

from listener import database
from topology import project


class TopologyTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.db=database(Path(self.tmp.name)/'x.sqlite')
    def tearDown(self):self.db.close();self.tmp.cleanup()
    def add(self, kind, name, address, asset, values, idx=0):
        self.db.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(
          '0x'+'1'*64,idx,10,'0x'+'2'*64,address,kind,name,asset,'2026-09-12T00:00:00+00:00',100,
          json.dumps(values),'{}',None));self.db.commit()
    def test_launch_builds_birth_and_relationships_idempotently(self):
        token='0x'+'a'*40;factory='0x'+'b'*40;curve='0x'+'c'*40;deployer='0x'+'d'*40;pair='0x'+'e'*40
        self.add('pons_v2','TokenLaunched',factory,token,{'token':token,'curve':curve,'deployer':deployer,'pairToken':pair})
        self.assertEqual(project(self.db),1);self.assertEqual(project(self.db),0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM topology_observations').fetchone()[0],1)
        edges={(r[0],r[1],r[2]) for r in self.db.execute('SELECT source,relation,target FROM topology_edges')}
        self.assertIn((deployer,'deployed',token),edges);self.assertIn((token,'trades_on',curve),edges)
    def test_v4_pool_links_currencies_and_hook(self):
        pool='0x'+'f'*64;c0='0x'+'1'*40;c1='0x'+'2'*40;hook='0x'+'3'*40
        self.add('v4','Initialize','0x'+'4'*40,pool,{'id':pool,'currency0':c0,'currency1':c1,'hooks':hook})
        project(self.db)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM topology_edges WHERE source=?",(pool,)).fetchone()[0],3)
        self.assertEqual(self.db.execute("SELECT observation_type FROM topology_observations").fetchone()[0],'NEW_POOL_BIRTH')

if __name__=='__main__':unittest.main()
