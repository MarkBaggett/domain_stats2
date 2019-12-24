import requests
import json
import random
import datetime

def dateconverter(o):
    if isinstance(o, datetime.datetime):
        return o.strftime("%Y-%m-%d %H:%M:%S")

def retrieve_isc(domain):
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
    
    fake_isc_response = json.dumps({"seen_by_web":fake_date2, "expires":fake_date3, "seen_by_isc":fake_date1, "alerts":[]}, default=dateconverter)
    #Process ISC response
    resp = json.loads(fake_isc_response)
    web = datetime.datetime.strptime(resp['seen_by_web'], '%Y-%m-%d %H:%M:%S')
    expire = datetime.datetime.strptime(resp['expires'], '%Y-%m-%d %H:%M:%S')
    isc = resp['seen_by_isc']
    if isc != "FIRST-CONTACT":
        isc = datetime.datetime.strptime(resp['seen_by_isc'], '%Y-%m-%d %H:%M:%S')
    return (web,expire, isc, resp['alerts']) 