import datetime as dt
from pathlib import Path
import tempfile
import threading
from http.server import ThreadingHTTPServer
import unittest
import urllib.request
import urllib.error
from dashboard import read, handler
from listener import database, Collector, set_meta, now
from test_listener import FakeRPC, make_log, LAUNCH, BUY, V2, addr

class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'data.sqlite'
    def tearDown(self): self.tmp.cleanup()
    def fixture(self):
        db=database(self.path)
        launch=make_log('pons_v2',0,LAUNCH,V2);buy=make_log('curve',0,BUY,addr(2));buy['logIndex']='0x1'
        c=Collector(db,FakeRPC([launch,buy]),start=2,chunk=1);c.setup();c.tick();db.close()
    def test_missing_database_does_not_create_file(self):
        self.assertEqual(read(self.path)['health']['state'],'waiting');self.assertFalse(self.path.exists())
    def test_live_aggregation_and_detail(self):
        self.fixture();data=read(self.path,addr(1));c=data['candidates'][0]
        self.assertEqual(c['buys'],1);self.assertEqual(c['events'],2)
        self.assertEqual(data['health']['state'],'healthy')
        self.assertEqual(data['events'][0]['decoded']['tokensOut'],str(10**35))
        self.assertFalse(data['has_more'])
    def test_stale_and_degraded_distinct(self):
        self.fixture();db=database(self.path)
        with db:set_meta(db,'heartbeat',(dt.datetime.now(dt.timezone.utc)-dt.timedelta(minutes=5)).isoformat())
        self.assertEqual(read(self.path)['health']['state'],'stale')
        with db:set_meta(db,'heartbeat',now());set_meta(db,'status','degraded')
        self.assertEqual(read(self.path)['health']['state'],'degraded');db.close()
    def test_read_does_not_modify_database(self):
        self.fixture();db=database(self.path);before=list(db.execute('SELECT * FROM meta'));db.close()
        read(self.path);db=database(self.path);after=list(db.execute('SELECT * FROM meta'));db.close()
        self.assertEqual([tuple(r) for r in before],[tuple(r) for r in after])
    def test_http_routes_validation_and_headers(self):
        self.fixture();server=ThreadingHTTPServer(('127.0.0.1',0),handler(self.path));thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        base=f'http://127.0.0.1:{server.server_port}'
        try:
            for route in ['/','/app.js','/style.css','/api/radar','/api/radar?asset='+addr(1)]:
                with urllib.request.urlopen(base+route) as r:
                    self.assertEqual(r.status,200);self.assertIn("default-src 'self'",r.headers['Content-Security-Policy'])
            for route,code in [('/.env',404),('/api/radar?asset=bad',400),('/api/radar?offset=-1',400)]:
                with self.assertRaises(urllib.error.HTTPError) as ctx:urllib.request.urlopen(base+route)
                self.assertEqual(ctx.exception.code,code)
        finally:server.shutdown();server.server_close();thread.join()

if __name__=='__main__':unittest.main()
