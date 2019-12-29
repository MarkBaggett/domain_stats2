import argparse
import expiring_cache
import database_functions
import webserver

parse_commandline
start_webserver

GOAL is the followign records

{seen_by_web:datetime,    Comes from local for 100M ISC for everything else
 seen_by_you: datetime    First seen by you
 seen_by_isc: Position in Top 100M OR datetime first seen by ISC
 Category: Established, NEW  
 FirstContacts: YOU, ISC, BOTH
 ISC_Other:  { }  Other alerts as provided by isc for this domain
}

#database record
d#Rank will contain ISC date for >100M records

#Cache Records ???   Just straight json answers or calculated?
#Cache is straight JSON responses.  CAN NOT CACHE anything with FIRSTCONTACT

def main_logic(domain):
    if domain in cache:
        cache_data =  cache.get(domain)
        #Could still be None as expiration is only determined upon get()
        if cache_data:
            return cache_data
    else:
        record = retrieve_database(domain)
        #TODO:   FIGURE OUT - Expire cache record when the domain record expires to force requisition? Expired records in DB go to ISC?
        if record:
            category = "NEW"
            if not expired and bornon> 2 years:
                category = "ESTABLISHED"
            if record.seen_by_you == "FIRSTCONTACT":
                record.seen_by_you = datetime.datetime.utcnow()
                alert_first_you = True
                update_record_in_database(domain, record)
            cache_data.set(domain, record, hours)
        else:
            #Even if the ISC responds with an error that still goes in the cache
            alert_first_you = True
            record = retrieve_isc(domain)
            if record:
                if record.seen_by_isc == "FIRSTCONTACT":
                    alert_first_isc = True
                    record.seen_by_isc = datetime.datetime.utcnow()
                cache.set(domain, record)
                update_record_in_database(domain,record)
            else:
                print("Handle ISC Error")
    return json_response(cache_data, category)
    



