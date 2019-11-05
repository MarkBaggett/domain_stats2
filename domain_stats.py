#!/usr/bin/env python3
#domain_stats.py by Mark Baggett
#Twitter @MarkBaggett

import http.server
import socketserver 
import socket
import urllib.parse
import sqlite3
import threading
import select
import re
import argparse
import sys
import time
import os
import datetime
import json
import pickle
import collections
import subprocess
import functools
import resource
import yaml
from dstat_utils import reduce_domain, load_config, get_creation_date, verify_domain
import logging

logging.basicConfig(filename="domain_stats.log", level=logging.DEBUG)



try:
    import whois
except Exception as e:
    print(str(e))
    print("You need to install the Python whois module.  Install PIP (https://bootstrap.pypa.io/get-pip.py).  Then 'pip install python-whois' ")
    sys.exit(0)
    

if os.system("which whois") != 0:
    print("You need to have whois installed on this machine.  Try 'apt install whois' ")
    sys.exit(0)

CacheRecord = collections.namedtuple("CacheRecord", 
                ["seen_by_web", "seen_by_us", "seen_by_you", "rank", "other"])

exec_semaphore = threading.Semaphore(2)

def my_lru_cache(maxsize=16384, cacheable = lambda _:True):
    #Create my own lru cache so I can remove items as needed
    def wrap_function_with_cache(function_to_call):
        _cache =  collections.OrderedDict()
        _CacheInfo = collections.namedtuple("CacheInfo", ["hits", "misses", "maxsize", "currsize","cache_bytes","app_kbytes"])
        lock = threading.RLock()
        hit = miss = 0

        def cache_info():
            nonlocal _cache
            """Report cache statistics"""
            with lock:
                return _CacheInfo(hit, miss, maxsize, len(_cache), sys.getsizeof(_cache),  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        def reset_info():
            nonlocal _cache
            hit = miss = 0


        def remove(*args):
            nonlocal _cache
            if args in _cache:
                with lock:
                    del _cache[args]

        def clear_all():
            nonlocal _cache
            with lock:
                _cache.clear()
             
        def newfunc(*args):
            nonlocal _cache, hit, miss  
            if args in _cache:
                hit += 1
                with lock:
                    _cache.move_to_end(args)
                return _cache.get(args)
            miss += 1
            ret_val = function_to_call(*args)
            #check to see if this should be cached
            if not cacheable(ret_val):
                return ret_val
            #otherwise update the cache
            with lock:
                _cache[args] = ret_val
                if len(_cache) > maxsize:
                    _cache.popitem(last=False)
            return ret_val

        def bypass_cache(*args):
            ret_val = function_to_call(*args)
            return ret_val

        newfunc.cache = _cache
        newfunc.cache_info = cache_info
        newfunc.remove = remove
        newfunc.reset_info = reset_info
        newfunc.clear_all = clear_all
        newfunc.bypass_cache = bypass_cache
        return newfunc
    return wrap_function_with_cache

def should_item_be_cached(cache_item):
    #If funciton returns False it is not cached
    #We don't want to cache blank database responses or responses that contain "FIRST-CONTACT"
    return cache_item and not "FIRST-CONTACT" in cache_item

def add_to_database( domain, seen_by_web, seen_by_us, seen_by_you, rank, other ):
    database_lock.acquire()
    try:
        db = sqlite3.connect("domain_stats.db")
        cursor = db.cursor()
        sql = "insert into domains (domain,seen_by_web, seen_by_us, seen_by_you, rank, other) values (?,?,?,?,?,?)"
        result = cursor.execute(sql, (domain, seen_by_web, seen_by_us, seen_by_you, rank, other) )
        db.commit()
    except Exception as e:
        import pdb;pdb.set_trace()
        logging.debug("Error occured writing to database. {}".format(str(e)))
    finally:
        database_lock.release()

def json_to_cacherec(json_record, **missing_args):
    rec = CacheRecord(**json_record, **missing_args)
    return rec
    
def dateconverter(o):
    if isinstance(o, datetime.datetime):
        return o.strftime("%Y-%m-%d %H:%M:%S")

def whois_to_cacherec(whois_record):
    "Given txt record return a cacherecord"
    domanrec = WhoisEntry.load(domain, whois_record)
    today = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #creation_date = re.findall("Creation", whois_record)
    creation_date = today
    crec = CacheRecord(creation_date, today, today, -1, "{}")
    return crec

def cacherec_to_json(cacherecord):
    cache_dict = cacherecord._asdict()
    cache_json = json.dumps(cache_dict, default=dateconverter)
    return cache_json

#def get_creation_date(rec):
#    born_on = rec.get("creation_date","invalid-creation_date")
#    if type(born_on) == list:
#        #Enhancement: Improve by fiding the most recent born on date
#        born_on = min(born_on)
#    return born_on

def load_config():
    with open("domain_stats.yaml") as fh:
        yaml_dict = yaml.safe_load(fh.read())
    Configuration = collections.namedtuple("Configuration", list(yaml_dict) )
    return Configuration(**yaml_dict)

def local_whois_query(domain,timeout=0):
    logging.debug("local whois query. {} {}".format(domain,timeout))
    try:
        whois_rec = whois.whois(domain, command=True)
    except Exception as e:
        logging.debug(f"Error During local whois query {str(e)}")
        return False
    if not whois_rec.get("domain_name"):
        logging.debug("Whois record didn't have a domain name. {}".format(whois_rec))
        return False
    logging.debug("whois type {}".format( type(whois_rec)))
    born_on = get_creation_date(whois_rec)
    if not born_on:
        logging.debug("No Born on date for. {} {}".format(domain, whois_rec) )
        return False
    today = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = CacheRecord( born_on,today,today,-1,"{}")
    if not timeout:
        return data
    logging.debug("Good Citizen")
    submit_data = {"action":"update","timeout":timeout,"data":json.dumps(whois_rec,default=dateconverter)}
    try:
        submit_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        submit_socket.settimeout(15)
        submit_socket.sendto(json.dumps(submit_data,default=dateconverter).encode(), (config.server_name,config.server_port))
    except Exception as e:
        logging.debug(f"Error submitting data to server {str(e)}")
    return data

def domain_stats(domain): 
    """ Given a domain return a tuple with  """
    """    (New to you (true or false), New to us (how new), New to all (born on date))   """
    """ response = { 
            "new2you": date put in local cache, 
            "new2us": date on udp server,
            new2all: bornon, 
            *Other fields to be determined (freq score, flagged as malicious)
            }
    """
    global config
    result = database_lookup(domain)
    #logging.debug("Initial database request result", result)
    if result:
        return cacherec_to_json(result)
    else:
        logging.info(f"to the web! {domain}")
        if not verify_domain(domain):
            return json.dumps({"error": f"error resolving dns {domain}"})
        query = json.dumps({"action":"query", "domain": domain}).encode()
        try:
            logging.info(f"making udp query {query}")
            udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_socket.settimeout(15)
            udp_socket.sendto(query, (config.server_name,config.server_port))
            #Only a single packet but use loop incase there are new lines in the data
            resp, addr = udp_socket.recvfrom(32768)
            logging.info(f"udp repsponse {resp}")
            today = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                #This should be a properly formatted json response
                json_response = json.loads(resp.decode())
            except Exception as e:
                logging.debug(f"Error parsing domain_stats server response {str(e)}")
                return False
            logging.debug("Version {}".format(json_response.get("version")))
            if "version" in json_response:
                if json_response.get("version") > config.database_version:
                    logging.debug("Version Change. Running update")
                    exit_code = os.system("python3 database_admin.py -u")
                    if exit_code == 0:
                        config = load_config()
                del json_response['version']
            if "error" in json_response:
                data = ""
                if json_response.get("error")=="server busy":
                    timeout = json_response.get("timeout",0)
                    #This one returns a cacherec
                    data = local_whois_query(domain,timeout)
                    logging.debug("Local whois exec {} {} ".format(timeout, data))
                if not data:
                    return json.dumps({"error": f"No whois record for {domain}"})
            else:                                                                  
                data = json_to_cacherec(json_response, seen_by_you = today,rank=-1)
            assert type(data) == CacheRecord
            #What we commit to database is different than what we return
            #Commit data to database will contain correct data information
            dbrec = data._asdict()
            #If this "FIRST-CONACT" from server put current date in database otherwise commit date received
            if dbrec.get("seen_by_us") == "FIRST-CONTACT":
                dbrec["seen_by_us"] = today
            #since we queried the server this must be the seen_by_you first contact
            dbrec["seen_by_you"] = today
            logging.info(f"adding to database {dbrec.items()}")
            add_to_database(domain, **dbrec)
            #Return a record contains "FIRST-CONTACT" instead of dates.  Subsequent queries will get db record with correct data
            return_rec = data._asdict()
            return_rec['seen_by_you'] = "FIRST-CONTACT"
            data = CacheRecord(**return_rec)
            return cacherec_to_json(data)
        except socket.timeout:
            logging.debug("Too much whois, too soon. Sleeping for a sec")
            time.sleep(1)
            return json.dumps({"error": f"busy {domain}"})
        except Exception as e:
            logging.debug(f"Error in udp query {str(e)}")
            data = local_whois_query(domain)
            if not data:
                return json.dumps({"error": f"No whois record for {domain}"})
            return cacherec_to_json(data)
        logging.debug("Hmm  how did i get here?")
        return f"This is bad. Not sure how I got here {domain} "


class domain_api(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.end_headers()
        (ignore, ignore, urlpath, urlparams, ignore) = urllib.parse.urlsplit(self.path)
        if re.search("[\/][\w.]*", urlpath):
            domain = re.search(r"[\/](.*)$", urlpath).group(1)
            #logging.debug(domain)
            if domain == "stats":
                result = str(database_lookup.cache_info()).encode()
            else:
                result = domain_stats(domain).encode()
            self.wfile.write(result)
        else:
            api_hlp = 'API Documentation\nhttp://%s:%s/domain.tld   where domain is a non-dotted domain and tld is a valid top level domain.' % (self.server.server_address[0], self.server.server_address[1])
            self.wfile.write(api_hlp.encode())
        return

    def log_message(self, format, *args):
        return

class ThreadedDomainStats(socketserver.ThreadingMixIn, http.server.HTTPServer):
    def __init__(self, *args,**kwargs):
        self.args = ""
        self.screen_lock = threading.Lock()
        self.exitthread = threading.Event()
        self.exitthread.clear()
        http.server.HTTPServer.__init__(self, *args, **kwargs)

config = load_config()

@my_lru_cache(maxsize = config.cached_max_items, cacheable = should_item_be_cached)
def database_lookup(domain):
    db = sqlite3.connect("domain_stats.db")
    cursor = db.cursor()
    logging.debug(f"I QUERIED THE DATABASE. NOT CACHE! for {domain}")
    result = cursor.execute("select seen_by_web, seen_by_us, seen_by_you,rank,other from domains where domain = ?" , (domain,) ).fetchone()
    #If we get a record UPDATE Database with date that you first queried tht domain
    if result:
        with database_lock:
            cursor.execute("update domains set seen_by_you=? where domain =?", (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), domain))
            db.commit()
        return CacheRecord(*result)
    return False

if __name__ == "__main__":
    try:
       serverip = socket.gethostbyname(config.server_name)
    except Exception as e:
       print(f"Unable to resolve {config.server_name}") 

    #Setup the server.
    database_lock = threading.Lock()
    server = ThreadedDomainStats((config.local_address, config.local_port), domain_api)

    #start the server
    print('Server is Ready. http://%s:%s/domain.tld' % (config.local_address, config.local_port))
    while True:
        try:
            server.handle_request()
        except KeyboardInterrupt:
            break

    print("Web API Disabled...")
    print("Control-C hit: Exiting server.  Please wait..")

