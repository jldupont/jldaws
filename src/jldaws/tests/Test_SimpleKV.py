"""
    Created on 2012-01-25
    @author: jldupont
"""
from nose.tools import raises


import jldaws.db.skv as skv
from jldaws.exceptions import SDB_Access

class Test():
    
    @classmethod
    def setup_class(self):
        """setup"""
        self.db=skv.SimpleKV()
        self.db.connect()
        self.db.set_domain("test")
    
    @classmethod    
    def teardown_class(self):
        """teardown"""
        
    def test_00delete_domain(self):
        self.db.delete_domain()
        self.db.set_domain("test")
        
    def test_01get_unknown(self):
        r=self.db.get("unknownkey")
        assert r is None

    def test_10insert(self):
        eid, _, _=self.db.insert("testkey", "testvalue")
        r=self.db.get("testkey", consistent_read=True)
        assert str(r.name) == str(eid)
        
    def test_11delete(self):
        count=self.db.delete("testkey")
        assert count==1
        
    def test_50category(self):
        eid, _, _=self.db.insert("catkey", "catvalue", category="cat")
        cr=self.db.get_by_category("cat", last=True, consistent_read=True)
        assert str(cr.name) == str(eid)
        
    @raises(SDB_Access)
    def test_99raises(self):
        self.db.delete_item(None, silent=False)