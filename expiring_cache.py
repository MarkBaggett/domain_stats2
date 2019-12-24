import collections
import datetime
import resource
import sys
import pickle
import threading


class ExpiringCache(collections.OrderedDict, collections.Counter):
    """This is a Least Recently Used Expiring Cache. Reading or setting a record makes it the most recently used.
       When an item is added such that the maxsize is exceeded the least recently used entry is dropped.
       Additionally entries will be marked with an expiration date.  If the expiration is exceeded None is retreieved
       and the entry is deleted when you query a value in the dictionary.""" 

    def __init__(self, maxsize = 65535, default_hours_to_live=720, *args, **kwargs):
        self.maxsize = maxsize
        #Set to -1 for no expirations and it behaves like a regular LRU cache 
        self.hours_to_live = default_hours_to_live
        self.hit = self.miss = self.expired = 0
        self.update_lock = threading.Lock()
        super().__init__()

    def cache_info(self):
        """Report cache statistics"""
        rpt =  f"""
HITS: {self.hit} \nMISS {self.miss}\nEXPIRED {self.expired}\nMax Size: {self.maxsize} (Current size is {len(self)})\n
Memory Usage: Cache Bytes={sys.getsizeof(self)} Application Kilobytes={resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}"""
        return rpt

    def reset_info(self):
        self.hit = self.miss = self.expired = 0

    def cache_dump(self, fname):
        with open(fname,"wb") as fhandle:
            pickle.dump(list(self.items()), fhandle, protocol=pickle.HIGHEST_PROTOCOL)

    def cache_load(self, fname):
        self.clear()
        with open(fname, "rb") as fhandle:
            other = pickle.load(fhandle)
            for key,val in other:
                with self.update_lock:
                    super().__setitem__(key, val)

    def get(self,key,default_value=None):
        retval = self[key]
        return retval or default_value

    def __getitem__(self, key):
        if key in self:
            expiration, read_count, data = super().__getitem__(key)
            del self[key]
            if expiration > datetime.datetime.utcnow() or (expiration < 0):
                self.hit +=1
                with self.update_lock:
                    super().__setitem__(key, (expiration, read_count+1, data))
                return data
            else:
                self.expired += 1
        else:
            self.miss +=1
        return None

    def enforce_size(self):
        """Delete items from front of dict (First in) unless it has an expiration of -2 until the length is < self.maxsize"""
        entries = iter(self.keys())
        with self.update_lock:
            while len(self) > self.maxsize:
                try:
                    tgt_key= next(entries)
                except StopIteration:
                    print("Unable to delete any keys but the maximum size is exceeded.  Ignoring Maxsize.")
                    break
                else:
                    expires,read_count,value = super().get(tgt_key)
                    if expires != -2:
                        del self[tgt_key]
                        entries = iter(self.keys())


    def set(self, key, value, update_expiration=True, reset_read_count=False, hours_to_live=None):
        """If update_expiration is True then the expiration date is updated on overwrites to existing keys"""
        """If you update an expired entry then a new expiration date is set on a new entry"""
        """If reset_read_count is True then read_count is set to sero on overwrites to existing  keys"""
        """Setting hours_to_live to -1 and they won't expire but can page out if least recently used."""
        """Set to -2 and they do not expire and the LRU can not remove them. Permanent entries in the cache (use with caution)"""
        """Set to 0 and the record is treated as already expired. nothing is added to the cache"""
        hours_to_live = hours_to_live or self.hours_to_live
        if hours_to_live == 0:
            return
        if hours_to_live < 0:
            expires = hours_to_live
        else:
            expires = datetime.datetime.utcnow() + datetime.timedelta(hours=hours_to_live)
        read_count = 0
        if key in self:
            with self.update_lock:
                current_expires,current_read_count,value = super().__getitem__(key)
                del self[key]
            if not update_expiration and (current_expires < datetime.datetime.utcnow()):
                expires = current_expires
            if not reset_read_count:
                read_count = current_read_count
        with self.update_lock:
            super().__setitem__( key, (expires, read_count, value))
        self.enforce_size()

    def __setitem__(self, key, value):
        if self.hours_to_live == 0:
            return
        if self.hours_to_live < 0:
            expires = self.hours_to_live
        else:
            expires = datetime.datetime.utcnow() + datetime.timedelta(hours=self.hours_to_live)
        read_count = 0
        if key in self:
            current_expires,read_count,value = super().__getitem__(key)
            del self[key]
            if current_expires < datetime.datetime.utcnow():
                expires = current_expires
        with self.update_lock:
            super().__setitem__( key, (expires, read_count, value))
        self.enforce_size()


def expiring_cache(maxsize=65535, cacheable = lambda _:True, hours_to_live=720):
    #Create my own lru cache so I can remove items as needed
    def wrap_function_with_cache(function_to_call):
        _cache =  Expiring_Cache(hours_to_live=hours_to_live, cacheable=cacheable, maxsize=maxsize)

        def newfunc(*args):
            nonlocal _cache
            if args in _cache:
                data = _cache.get(args)
                return data
            ret_val = function_to_call(*args)
            #check to see if this should be cached
            if cacheable(ret_val):
                _cache[args] =  ret_val
            return ret_val

        def bypass_cache(*args):
            ret_val = function_to_call(*args)
            return ret_val

        newfunc.cache = _cache
        newfunc.reset_info = _cache.reset_info
        newfunc.cache_dump = _cache.cache_dump
        newfunc.cache_load = _cache.cache_load
        newfunc.cache_info = _cache.cache_info
        newfunc.bypass_cache = bypass_cache
        return newfunc
    return wrap_function_with_cache