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
import sys
import database_io
import network_io
import config
import expiring_cache
import urllib

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
        if not latest_version:
            print(f"Unable to connect to {config.server_name} on port {config.server_port} to determine version.")
            sys.exit(1)
    except Exception as e:
        print(f"Unable to connect to {config.server_name} on port {config.server_port} to determine version. {str(e)}")
        sys.exit(1)
    return str(latest_version)
    
def reset_database(version_number):
    global config
    tgt_major, tgt_minor = map(int, str(version_number).split("."))
    config = update_config(database_version = f"{tgt_major}.0" )
    if pathlib.Path(config.database_file).exists():
        pathlib.Path(config.database_file).rename("{}.archive-{}".format(config.database_file, datetime.datetime.now()))
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
            print("\r|{0:-<50}| {1:3.2f}%".format("X"*( 50 * pos//num_recs), 100*pos/num_recs),end="")
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
    print(f"Updating from {config.database_version} to {latest_version}")
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
        process_domain_updates(str(dst_path), verify=args.verify)
        config = update_config(database_version= version)

if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument('-f','--firstcontacts',action="store_true",required=False,help='Reset all domains to First-Contact on the local system (seen-by-me)')
    parser.add_argument('-c','--create',action="store_true",required=False,help='Create the specified database. (Erases and overwrites existing files.)')
    parser.add_argument('-u','--update',action="store_true", required=False,help='Update the database established domains.')
    parser.add_argument('-v','--version',action="store_true", required=False,help='Check database version')
    parser.add_argument('filename', help = "The name or path/name to the sqlite database to perform operations on.")
 
    args = parser.parse_args()

    config = config.config("domain_stats.yaml")
    database = database_io.DomainStatsDatabase(args.filename)
    server_config = network_io.get_server_config()
    cache = expiring_cache.ExpiringCache()


    if args.create:
        if input("Are you sure?  This will destroy any existing file with that name.").lower().startswith("y"):
            database.create_file(args.filename)
        else:
            print("aborting.")
            sys.exit(0)
    if args.update:
        critical, interval, messages = network_io.health_check(1.0, database.version, cache, database.stats)
        if critical:
            if messages[0] == 'UPDATE-DATABASE':
                database.update_database(messages[1], config)
    if args.firstcontacts:
        reset_first_contact()
    if args.version:
        critical, interval, messages = network_io.health_check(1.0, database.version, cache, database.stats)
        server_version = database.version
        if critical:
            if messages[0] == 'UPDATE-DATABASE':
                server_version = messages[1]
        print(f"Local Version:{database.version}  Server Version:{server_version}")

    