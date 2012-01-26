'''
    Simple Key/Value store organized as a "journal"
    
    * 'insert' timestamped KV pairs
    * retrieve list of timestamped KV pairs 

    Created on 2011-06-20
    @author: jldupont
'''
import os
import uuid
import boto
from jldaws.tools_date import iso_now_and_then
from jldaws.exceptions import SDB_Connect, SDB_CreateDomain, SDB_Access, SDB_CreateItem


class SimpleKV():
    
    def __init__(self, aws_access_key=None, aws_secret_key=None):
        self.aws_access_key=aws_access_key or os.environ.get("AWS_ACCESS_KEY_ID", None)
        self.aws_secret_key=aws_secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY", None)
        
    def connect(self):
        """
        Connect to AWS SDB
        
        Use this method first before attempting any other
        
        @raise SDB_Connect:  
        """
        try:        
            if self.aws_access_key is None:
                self.sdbconn=boto.connect_sdb()
            else:
                self.sdbconn=boto.connect_sdb(self.aws_access_key, self.aws_secret_key)
        except:
            try:    self.sdbconn=boto.connect_sdb()
            except: raise SDB_Connect()        
            
    def set_domain(self, domain_name):
        """
        Set the domain on which to work with
        
        @raise SDB_CreateDomain: 
        """
        self.dname=domain_name
        
        try:        self.sdb=self.sdbconn.create_domain(self.dname)
        except:
            try:    self.sdb=self.sdbconn.create_domain(self.dname)
            except: raise SDB_CreateDomain("Can't create SDB domain '%s'" % self.dname)
            
    def delete_domain(self):
        """
        Delete domain  (silent)
        """
        try:
            self.sdbconn.delete_domain(self.dname)
        except:
            pass
            
    def get(self, key, category=None, last=True, limit=20, consistent_read=False):
        """
        Get a specific from, optionally, a given category
        
        @raise SDB_Access:
        """
        if category is None:
            stm="SELECT * from %s where key='%s' and creation_date is not null order by creation_date DESC limit %s" % (self.dname, key, limit)
        else:
            stm="SELECT * from %s where key='%s' and category='%s' and creation_date is not null order by creation_date DESC limit %s" % (self.dname, key, category, limit)
            
        try:                
            rs=self.sdb.select(stm, max_items=limit, consistent_read=consistent_read)
        except:
            raise SDB_Access()

        if last:
            try:    return rs.next()
            except: return None

        return rs
        
    def get_by_category(self, category, limit=20, consistent_read=False, last=True):
        """
        Get a batch of items from a given category
        
        :param category: string
        
        @raise SDB_Access: 
        """
        stm="SELECT * from %s where category='%s' and creation_date is not null order by creation_date DESC limit %s" % (self.dname, category, limit)
        try:
            rs=self.sdb.select(stm, max_items=limit, consistent_read=consistent_read)
        except:
            raise SDB_Access()
        
        if last:
            try:    return rs.next()
            except: return None
        
        return rs

    def delete_item(self, item, silent=True):
        try:
            self.sdb.delete_item(item)
        except:
            if not silent:
                raise SDB_Access()

    def delete(self, key):
        """
        Must find all items associated with 'key' and delete 1 by 1
        
        @return Number of items deleted
        @raise SDB_Access: 
        """
        try:
            s=self.get(key, last=False, consistent_read=True)
            if s is None:
                return 0
            count=0
            for item in s:
                self.sdb.delete_attributes(item.name)
                count=count+1
                return count            
        except Exception, e:
            raise SDB_Access(e)
           
    def del_by_value(self, value, category=None, consistent_read=True, silent=True):
        """
        Deletes entries by 'value' & optionally 'category'
        
        @raise SDB_Access: 
        """
        if category is None:
            stm="SELECT * from %s where value='%s' and creation_date is not null order by creation_date DESC" % (self.dname, value)
        else:
            stm="SELECT * from %s where value='%s' and category='%s' and creation_date is not null order by creation_date DESC" % (self.dname, value, category)
            
        try:                
            rs=self.sdb.select(stm, consistent_read=consistent_read)
        except:
            if silent:
                return 0
            else:
                raise SDB_Access()
            
        try:
            count=0
            for item in rs:
                self.delete_item(item)
                count=count+1
            
            return count
        except:
            if not silent:
                raise SDB_Access()
            return 0
        
            
    def del_by_key_value(self, key, value, category=None, silent=True):
        """
        Deletes a specific entry in the journal
        
        @return # deleted
        @raise SDB_Access: 
        """
        if category is None:
            stm="SELECT * from %s where key='%s' and value='%s' and creation_date is not null order by creation_date DESC" % (self.dname, key, value)
        else:
            stm="SELECT * from %s where key='%s' and value='%s' and category='%s' and creation_date is not null order by creation_date DESC" % (self.dname, key, value, category)
            
        try:                
            rs=self.sdb.select(stm)
            count=0
            for item in rs:
                self.delete_item(item)
                count=count+1
            
            return count            
        except:
            if silent:
                return 0
            else:
                raise SDB_Access()

            
    def insert(self, key, value, attrs=None, category=None, id=None, days=0, hours=0, minutes=0):
        """
        Inserts a [category:key:value] with an optional expiry_date
        
        @return (id, creation iso time, expiration iso time)        
        """
        id=id or uuid.uuid1()

        try:     
            item = self.sdb.new_item(id)
        except:  
            raise SDB_CreateItem("Can't create SDB key(%s) in '%s' domain" % (key, self.dname))
        
        now, then=iso_now_and_then(days=days, hours=hours, minutes=minutes)
        
        item["creation_date"]=now
        item["key"]=key
        item["value"]=value
        if category is not None:
            item["category"]=category
        if then is not None:
            item["expiry_date"]=then
        
        ## Can't use 'extend' as the object 'item' isn't a real dict
        if attrs is not None:
            for k,v in attrs.iteritems():
                item[k]=v
        
        try:    item.save()
        except Exception,e: 
            raise SDB_Access("Can't save item with SDB key(%s) in domain(%s): %s" % (key, self.dname, e))
        
        return (str(id), now, then)

    def get_batch_by_value(self, value, category=None, limit=1000, consistent_read=False):
        """
        Returns a 'batch' of entries based on category & value
        Oldest first
        """
        if category is None:
            stm="SELECT * from %s where value='%s' and creation_date is not null order by creation_date asc limit %s" % (self.dname, value, limit)
        else:
            stm="SELECT * from %s where category='%s' and value='%s' and creation_date is not null order by creation_date asc limit %s" % (self.dname, category, value, limit)
        try:
            rs=self.sdb.select(stm, max_items=limit, consistent_read=consistent_read)
            return rs
        except:
            raise SDB_Access()


    def get_batch(self, limit=1000, by_expiry_date=False, consistent_read=False):
        """
        Returns a 'batch' based on ASC order
        
        NOTE: Legacy... use the more versatile :get_by_batch
        """
        if by_expiry_date:
            stm="SELECT * from %s where expiry_date is not null order by expiry_date ASC limit %s" % (self.dname, limit)
        else:
            stm="SELECT * from %s where creation_date is not null order by creation_date asc limit %s" % (self.dname, limit)
        try:
            rs=self.sdb.select(stm, consistent_read=consistent_read, max_items=limit)
        except:
            raise SDB_Access()
        
        return rs

    def get_by_batch(self, start=0, limit=1000, dir='asc', consistent_read=False):
        """
        Retrieves a batch of record based on 'creation_date'
        """
        #logging.info("kv.get_by_batch: start(%s) limit(%s)" % (start, limit))
        stm="select * from %s where creation_date>'%s' order by creation_date %s limit %s" % (self.dname, start, dir, limit)
        try:
            rs=self.sdb.select(stm, consistent_read=consistent_read, max_items=limit)
        except:
            raise SDB_Access()
        
        return rs


