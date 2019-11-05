import sqlite3
import yaml
import collections
import requests
import argparse
import json
import socket
import threading
import pathlib
import datetime
import urllib
from dstat_utils import reduce_domain, load_config, verify_domain, new_domain, get_db, update_config


createstr="""
CREATE TABLE domains (
 domain_id INTEGER PRIMARY KEY AUTOINCREMENT,
 domain text NOT NULL UNIQUE,
 seen_by_web timestamp not NULL,
 seen_by_us timestamp not NULL,
 seen_by_you timestamp not NULL,
 rank INTEGER DEFAULT -1,
 other BLOB
);
"""


#sqlite dump establishe to txt
#.headers off
#.mode csv
#.output 2.txt
#select domain from domains where seen_by_web <  date('now','-2 years');
#.quit


database_file = "domain_stats.db"
lock = threading.Lock()

def create_tables():
    datab = get_db()
    cursor = datab.cursor()
    cursor.execute(createstr)
    datab.commit();

def check_update():
    query = json.dumps({"action":"version"}).encode()
    try:
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.settimeout(4)
        udp_socket.sendto(query, (config.server_name, int(config.server_port)))
        #Only a single packet but use loop incase there are new lines in the data
        resp, addr = udp_socket.recvfrom(32768)
        vresp = json.loads(resp.decode())
        latest_version = vresp.get("version")
    except Exception as e:
        print(f"udp query error {str(e)}")
    return str(latest_version)
    
def reset_database(version_number):
    global config
    tgt_major, tgt_minor = map(int, str(version_number).split("."))
    config = update_config(database_version = f"{tgt_major}.0" )
    if pathlib.Path(database_file).exists():
        pathlib.Path(database_file).rename("domain_stats.db.archive-{}".format(datetime.datetime.now()))
    create_tables()
    get_updates(version_number)

def reset_first_contact():
    db= get_db()
    cursor = db.cursor()
    cursor.execute("update domains set seen_by_you=?", ("FIRST-CONTACT",))
    db.commit()

def process_domain_updates(update_file,verify=True):
    new_domains = open(update_file).readlines()
    num_recs = len(new_domains)
    db=get_db()
    cursor = db.cursor()    
    for pos,entry in enumerate(new_domains):
        if pos % 50 == 0:
            print("\r|{0:-<50}| {1:3.2f}%".format("X"*( 50 * pos//num_recs), 100*pos//num_recs),end="")
        rank, domain = entry.split(",")
        odomain = domain.strip()
        domain = reduce_domain(odomain)
        if verify and not verify_domain(domain):
            continue
        new_domain(cursor,rank,domain)
    db.commit()
    print("\r|XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX| 100.00% FINISHED")

def get_updates(latest_version):
    global config
    latest_major, latest_minor = map(int, str(latest_version).split("."))
    current_major, current_minor = map(int, str(config.database_version).split("."))
    print(f"Updating form {config.database_version} to {latest_version}")
    if latest_major > current_major:
        print("WARNING: Domain Stats database is a major revision behind. Database required rebuild.")
        return None
    target_updates = range(current_minor+1, latest_minor+1 )
    for update in target_updates:
        version = f"{current_major}.{update}"
        print(f"Now applying update {version}")
        tgt_url = f"{config.target_updates}/{current_major}/{update}.txt" 
        dst_path = pathlib.Path().cwd() / "data" / f"{current_major}" / f"{update}.txt"
        urllib.request.urlretrieve(tgt_url, str(dst_path))
        process_domain_updates(str(dst_path), verify=False)
        config = update_config(database_version= version)

if __name__ == "__main__":
    config = load_config()

    parser=argparse.ArgumentParser()
    parser.add_argument('-f','--firstcontacts',action="store_true",required=False,help='Reset all domains to First-Contact on the local system (seen-by-me)')
    parser.add_argument('--rebuild',action="store_true",required=False,help='Erase and rebuild the entire database to the latest version')
    parser.add_argument('-u','--update',action="store_true", required=False,help='Update the database established domains.')
    parser.add_argument('-v','--version',action="store_true", required=False,help='Check database version')
   
    args = parser.parse_args()

    if args.firstcontacts:
        reset_first_contact()
    elif args.rebuild:
        if input("Are you sure?  This will destroy the database.").lower().startswith("y"):
            reset_database(check_update())
    elif args.update:
        get_updates(check_update())
    elif args.version:
        print(f"Local Version:{config.database_version}  Server Version:{check_update()}")

    