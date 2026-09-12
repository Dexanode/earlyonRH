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
    def test_long_create_links_asset_quote_and_pool(self):
        token='0x'+'a'*40;quote='0x'+'b'*40;pool='0x'+'c'*40;airlock='0x'+'d'*40
        self.add('long','Create',airlock,token,{'asset':token,'numeraire':quote,'initializer':'0x'+'e'*40,'poolOrHook':pool})
        project(self.db)
        edges={(r[0],r[1],r[2]) for r in self.db.execute('SELECT source,relation,target FROM topology_edges')}
        self.assertIn((token,'trades_on',pool),edges);self.assertIn((token,'paired_with',quote),edges)
    def test_unknown_factory_pool_birth_builds_generic_topology(self):
        factory='0x'+'5'*40;pool='0x'+'6'*40;t0='0x'+'7'*40;t1='0x'+'8'*40
        self.add('v3_factory','PoolCreated',factory,pool,{'token0':t0,'token1':t1,'fee':'3000','tickSpacing':'60','pool':pool})
        project(self.db)
        self.assertEqual(self.db.execute('SELECT protocol FROM topology_entities WHERE id=?',(pool,)).fetchone()[0],'dex_factories')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM topology_edges WHERE source=? AND relation="contains"',(pool,)).fetchone()[0],2)
    def test_erc6551_account_links_to_collection(self):
        registry='0x'+'9'*40;account='0x'+'a'*40;collection='0x'+'b'*40
        self.add('erc6551_registry','ERC6551AccountCreated',registry,account,{'account':account,'implementation':'0x'+'c'*40,'salt':'0x'+'0'*64,'chainId':'4663','tokenContract':collection,'tokenId':'44'})
        project(self.db)
        row=self.db.execute('SELECT relation,target FROM topology_edges WHERE source=?',(collection,)).fetchone()
        self.assertEqual(tuple(row),('owns_account',account))

if __name__=='__main__':unittest.main()
