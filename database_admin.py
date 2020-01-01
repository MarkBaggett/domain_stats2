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
    cache = expiring_cache.ExpiringCache()
    isc_connection = network_io.IscConnection()


    if args.create:
        if input("Are you sure?  This will destroy any existing file with that name.").lower().startswith("y"):
            database.create_file(args.filename)
        else:
            print("aborting.")
            sys.exit(0)
    if args.update:
        min_client, min_data = isc_connection.get_config()
        if database.version < min_data:
            print(f"Database is out of date.  Forcing update from {database.version} to {min_data}")
            database.update_database(min_data, config['target_updates'])
    if args.firstcontacts:
        database.reset_first_contact()
    if args.version:
        min_client, min_data = isc_connection.get_config()
        server_version = database.version
        print(f"Local Version:{database.version}  Server Version:{min_data}")

    