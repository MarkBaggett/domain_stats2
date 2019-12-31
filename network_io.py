import requests
import json
import random
import datetime
import logging


log = logging.getLogger(__name__)
logfile = logging.FileHandler('domain_stats.log')
logformat = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
logfile.setFormatter(logformat)

#to isc
#{"command" : "query",  "domain": toplevel.tld}   
#response:
    ### ISC Response is a JSON packet in the following format:
    # { "seen_by_web" =  Domain Created Date in format YYYY-mm-DD HH:MM:SS,   (If domain has multiple dates this is the most recent)
    #   "expires" = domain expiration date Date in format YYYY-mm-DD HH:MM:SS,  (if domain has multiple dates this is the most recent)
    #    "seen_by_isc" = date when ISC first queried whoisxml to build this record in format YYYY-mm-DD HH:MM:SS,als
    #     "alerts"  =  A list of strings to make the user aware of regarding the domain in question (for later use) formta is ['alert1','alert2']
    ##STub an ISC response

#When a client starts it sends a status to isc.  ISC can force database updates on client, deny client access
#To isc
#{"command": "status", "client_version":"client version number","database_version":database version info,  cache_efficiency": [hit,miss,expired], "database_efficiency": [hit,miss]}
#response:
# {"current_database_version": "database version expected", "interval": "number of seconds ISC wishes to wait until next status update", "deny_client": "client message"}

def health_check(client_version, database_version, cache, database_stats):
    #To isc
    #submit = json.dumps({"command":"status", "client_version":client_version, "database_version":database_version, "cache_efficiency":[cache.hits, cache.miss, cache.expire], "database_efficiency":[database_stats.hits, database_stats.miss])
    #{"command": "status", "client_version":"client version number","database_version":database version info,  cache_efficiency": [hit,miss,expired], "database_efficiency": [hit,miss]}
    #response from isc
    # {"expected_database_version": "database version expected","expected_client_version":client version expected,  "interval": "number of minutes ISC wishes to wait until next status update", "deny_client": "client message", "notice":['messages' 'to' 'client']}
    #functions returns tuple:
    #       Position 1 of Tuple is boolean representing "CRITICAL ERROR" which, when True causes the program to abort.
    #       a list of messages to give to the clients regarding its health and operating ability
    client_messages = []
    fake_isc_response = json.dumps({"expected_database_version":1.0, "expected_client_version":1.0, "interval":5, "deny_client":"", "notice":['message1']})
    isc_response = json.loads(fake_isc_response)
    expected_database = isc_response.get("expected_database_version")
    if expected_database != database_version:
        log.debug(f"Database is out of date. ISC expected {expected_database}. Running {database_version}")
        return ( True, 0.1 , ["UPDATE-DATABASE", str(expected_database)] ) 
    expected_client = isc_response.get("expected_client_version")
    if expected_client != client_version:
        log.debug(f"Warning Client Software is out of data.")
        client_messages.append("Your version of domain_stats is out of date.  Please update.")
    deny_client = isc_response.get("deny_client")
    if deny_client:
        client_messages.insert(0, deny_client)
    client_messages.extend(isc_response.get("notice"))
    return ( deny_client, isc_response.get("interval", 60), client_messages )


def dateconverter(o):
    if isinstance(o, datetime.datetime):
        return o.strftime("%Y-%m-%d %H:%M:%S")

def retrieve_isc(domain):
    #to isc
    #{"command" : "query",  "domain": toplevel.tld} 
    ### ISC Response is a JSON packet in the following format:
    # { "seen_by_web" =  Domain Created Date in format YYYY-mm-DD HH:MM:SS,   (If domain has multiple dates this is the most recent)
    #   "expires" = domain expiration date Date in format YYYY-mm-DD HH:MM:SS,  (if domain has multiple dates this is the most recent)
    #    "seen_by_isc" = date when ISC first queried whoisxml to build this record in format YYYY-mm-DD HH:MM:SS,als
    #     "alerts"  =  A list of strings to make the user aware of regarding the domain in question (for later use) formta is ['alert1','alert2']
    ##STub an ISC response
    fake_date1 = (datetime.datetime.now() - datetime.timedelta(days=random.randrange(365,3000))).replace(microsecond=0).isoformat().replace("T"," ")
    fake_date1 = random.choice([fake_date1, "FIRST-CONTACT"])
    fake_date2 = (datetime.datetime.now() - datetime.timedelta(days=random.randrange(365,3000))).replace(microsecond=0).isoformat().replace("T"," ")
    fake_date3 = (datetime.datetime.now() + datetime.timedelta(days=random.randrange(365,3000))).replace(microsecond=0).isoformat().replace("T"," ")
    fake_isc_response1 = json.dumps({"seen_by_web":fake_date2, "expires":fake_date3, "seen_by_isc":fake_date1, "alerts":[]}, default=dateconverter)
    #ISC can also generate Error responses and define how long the error is cached on the client (preventing repeated queries)
    #Those responses look like this:
    #{ "seen_by_web" = "ERROR",
    #   "expires = "ERROR"
    #   "seen_by_isc = hours to live for this error in client cache for the queried domain. 0=dont cache,-1=dont expire but allow lru,-2=permenant (use with caution)"
    #   "alerts" = ['alert1','alert2' ]  list if alerts to associate with this query.  }
    fake_error_ttl = random.randrange(-1,5)
    alertoptions= ['bad thing happened', 'domain doesnt exist','login expired','isc temporarily unavailable', 'bad domain','bad record from registrar','unable to connect to whois']
    fake_alerts = [random.choice(alertoptions) for _ in range(random.randrange(1,3))]
    fake_isc_response2 = json.dumps({"seen_by_web":"ERROR", "expires":"ERROR", "seen_by_isc":fake_error_ttl, "alerts":fake_alerts}, default=dateconverter)
    fake_isc_response = random.choice([ fake_isc_response1]*5 + [fake_isc_response2])
    #Process ISC response
    resp = json.loads(fake_isc_response)
    web = resp['seen_by_web']
    if web != "ERROR":
        web = datetime.datetime.strptime(web, '%Y-%m-%d %H:%M:%S')
        expire = datetime.datetime.strptime(resp['expires'], '%Y-%m-%d %H:%M:%S')
        isc = resp['seen_by_isc']
        if isc != "FIRST-CONTACT":
            isc = datetime.datetime.strptime(isc, '%Y-%m-%d %H:%M:%S')
        return (web,expire, isc, resp['alerts'])
    return ("ERROR","ERROR", resp['seen_by_isc'], resp['alerts'])