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
import pathlib
import yaml
import code
from dstat_utils import reduce_domain, load_config, get_creation_date, verify_domain
import logging

logging.basicConfig(filename="domain_stats.log",format='%(asctime)s %(levelname)-8s %(message)s',level=logging.DEBUG)



try:
    import whois
except Exception as e:
    print(str(e))
    print("You need to install the Python whois module.  Install PIP (https://bootstrap.pypa.io/get-pip.py).  Then 'pip install python-whois' ")
    sys.exit(0)
    
if os.system("which whois") != 0:
    print("You need to have whois installed on this machine.  Try 'apt install whois' ")
    sys.exit(0)

config = load_config()

dbpath = pathlib.Path().cwd() / config.database_file
if not dbpath.exists():
    print("No database was found. Try running database_admin.py --rebuild to create it.")
    sys.exit(0)


exec_semaphore = threading.Semaphore(2)

def my_lru_cache(maxsize=16384, cacheable = lambda _:True, days_to_live=7):
    #Create my own lru cache so I can remove items as needed
    def wrap_function_with_cache(function_to_call):
        _cache =  collections.OrderedDict()
        _CacheInfo = collections.namedtuple("CacheInfo", ["hits", "misses", "expired" ,"maxsize", "currsize","cache_bytes","app_kbytes"])
        lock = threading.RLock()
        hit = miss = expired = 0

        def cache_info():
            nonlocal _cache
            """Report cache statistics"""
            with lock:
                return _CacheInfo(hit, miss, expired, maxsize, len(_cache), sys.getsizeof(_cache),  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        def reset_info():
            nonlocal _cache, hit, miss, expired
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
            nonlocal _cache, hit, miss, expired
            if args in _cache:
                expiration, data = _cache.get(args)
                if expiration > datetime.datetime.now():
                    hit += 1
                    with lock:
                        _cache.move_to_end(args)
                    return data
                else:
                    expired += 1
                    with lock:
                        del _cache[args]
            miss += 1
            ret_val = function_to_call(*args)
            #check to see if this should be cached
            if not cacheable(ret_val):
                return ret_val
            expiration = ret_val.get("expiration") or datetime.datetime.now() + datetime.timedelta(days = days_to_live)
            #otherwise update the cache
            with lock:
                _cache[args] = (expiration, ret_val)
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
    return cache_item and not "FIRST-CONTACT" in cache_item.values()

def add_to_database( domain, seen_by_web, seen_by_us, seen_by_you, rank, other, error="",expiration="" ):
    database_lock.acquire()
    try:
        db = sqlite3.connect(config.database_file)
        cursor = db.cursor()
        sql = "insert into domains (domain,seen_by_web, seen_by_us, seen_by_you, rank, other) values (?,?,?,?,?,?)"
        result = cursor.execute(sql, (domain, seen_by_web, seen_by_us, seen_by_you, rank, json.dumps(other)) )
        db.commit()
    except Exception as e:
        logging.debug("Error occured writing to database. {}".format(str(e)))
    finally:
        database_lock.release()

def new_cache_entry(seen_by_web, seen_by_us, seen_by_you, rank=-1, other={}, ttl = 0):
    cache_entry = {'seen_by_web':seen_by_web,'seen_by_us':seen_by_us,'seen_by_you':seen_by_you,'rank':rank, 'other':other }
    if ttl:
        cache_entry['expiration'] = datetime.datetime.now() + datetime.timedelta(min = ttl)
    return cache_entry

def error_response(error_msg, expiration=""):
    expire = expiration or datetime.datetime.now() + datetime.timedelta(minutes=10)
    return {"error": error_msg, "expiration":expire}

def dateconverter(o):
    if isinstance(o, datetime.datetime):
        return o.strftime("%Y-%m-%d %H:%M:%S")

def database_lookup(domain):
    db = sqlite3.connect(config.database_file)
    cursor = db.cursor()
    logging.debug(f"I QUERIED THE DATABASE. NOT CACHE! for {domain}")
    result = cursor.execute("select seen_by_web, seen_by_us, seen_by_you,rank,other from domains where domain = ?" , (domain,) ).fetchone()
    #If we get a record UPDATE Database with date that you first queried tht domain
    if result:
        with database_lock:
            cursor.execute("update domains set seen_by_you=? where domain =?", (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), domain))
            db.commit()
        return new_cache_entry(*result)
    return False

def local_whois_query(domain,timeout=0):
    logging.debug("local whois query. {} {}".format(domain,timeout))
    perm_error = datetime.datetime.now() + datetime.timedelta(days=30)
    try:
        whois_rec = whois.whois(domain, command=True)
    except Exception as e:
        logging.debug(f"Error During local whois query {str(e)}")
        return error_response(f"Unable to run whois locally")
    if not whois_rec.get("domain_name"):
        logging.debug("Whois record didn't have a domain name. {}".format(whois_rec))
        return error_response(f"whois record missing domain name", perm_error) 
    born_on = get_creation_date(whois_rec)
    if not born_on:
        logging.debug("No Born on date for. {} {}".format(domain, whois_rec) )
        return error_response(f"whois record has no creation date", perm_error)
    today = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = new_cache_entry(born_on,today,today,-1,{})
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

@my_lru_cache(maxsize = config.cached_max_items, cacheable = should_item_be_cached)
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
        return result
    else:
        expire_now = datetime.datetime.now()
        perm_error = datetime.datetime.now() + datetime.timedelta(days=30)
        if not verify_domain(domain):
            return error_response(f"error resolving dns {domain}")
        logging.info(f"to the web! {domain}")
        query = json.dumps({"version":config.database_version,"action":"query", "domain": domain}).encode()
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
                server_response = json.loads(resp.decode())
            except Exception as e:
                logging.debug(f"Error parsing domain_stats server response {str(e)}")
                return False
            logging.debug("Version {}".format(server_response.get("version")))
            if "version" in server_response:
                if server_response.get("version") > config.database_version:
                    logging.debug("Version Change. Running update")
                    exit_code = os.system("python3 database_admin.py -u")
                    if exit_code == 0:
                        config = load_config()
                del server_response['version']
            if "error" in server_response:
                timeout = server_response.get("timeout",0)
                data = local_whois_query(domain,timeout)
                logging.debug("Local whois exec {} {} ".format(timeout, data))
                if 'error' in data:
                    return data
            else:
                logging.debug(f"server_response: {server_response} type:{type(server_response)}")                                                                  
                data = new_cache_entry(**server_response, seen_by_you = today,rank=-1)

            #What we commit to database is different than what we return
            #Commit data to database will contain correct data information
            #If this "FIRST-CONACT" from server put current date in database otherwise commit date received
            to_database = dict(data)
            if "error" in to_database:
                logging.debug(f"Unexpected error key in data {data}")
                del to_database['error']
            if "expiration" in to_database:
                logging.debug(f"Unexpected expiration key in data {data}")
                del to_database['expiration']
            if to_database.get("seen_by_us") == "FIRST-CONTACT":
                to_database["seen_by_us"] = today
            #since we queried the server this must be the seen_by_you first contact
            to_database["seen_by_you"] = today
            logging.info(f"adding to database {to_database.items()}")
            add_to_database(domain, **to_database)
            #Return a record contains "FIRST-CONTACT" instead of dates.  Subsequent queries will get db record with correct data
            data['seen_by_you'] = "FIRST-CONTACT"
            return data
        except socket.timeout:
            logging.debug("Too much whois, too soon. Sleeping for a sec")
            time.sleep(1)
            return error_response( f"busy {domain}")
        except Exception as e:
            logging.debug(f"Error in udp query {str(e)}")
            #last change try localwhois
            data = local_whois_query(domain)
            print("")
            if "error" in data:
                return data
            return new_cache_entry(**data)
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
                result = str(domain_stats.cache_info()).encode()
            else:
                domain = reduce_domain(domain)
                result = json.dumps(domain_stats(domain), default=dateconverter).encode()
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

#begin paste
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True

    try:
        server_thread.start()
        #code.interact(local=locals())
        while True: time.sleep(100)
    except (KeyboardInterrupt, SystemExit):
        server.shutdown()
        server.server_close()
        
    print("Web API Disabled...")
    print("Control-C hit: Exiting server.  Please wait..")

