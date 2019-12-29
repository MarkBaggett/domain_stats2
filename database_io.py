import sqlite3
import threading
import datetime
import logging

database_lock = threading.Lock()

  
class database_stats:
    def __init__(self, hit=0,miss=0, inserts=0):
        self.hit = hit
        self.miss = miss
        self.insert = inserts

    def __repr__(self):
        return f"database_stats(hit={self.hit},miss={self.miss},insert={self.insert})"

def add( domain, seen_by_web, seen_by_isc, seen_by_you, rank, other, error="",expiration="" ):
    database_lock.acquire(database_filename)
    try:
        db = sqlite3.connect(database_filename)
        cursor = db.cursor()
        sql = "insert into domains (domain,seen_by_web, seen_by_us, seen_by_you, rank, other) values (?,?,?,?,?,?)"
        result = cursor.execute(sql, (domain, seen_by_web, seen_by_us, seen_by_you, rank, json.dumps(other)) )
        db.commit()
    except Exception as e:
        log.debug("Error occured writing to database. {}".format(str(e)))
    finally:
        database_lock.release()

def update(database_filename, domain, record_seen_by_web, record_expires, record_seen_by_isc, record_seen_by_you):
    record_seen_by_web = record_seen_by_web.strftime('%Y-%m-%d %H:%M:%S')
    record_expires = record_expires.strftime('%Y-%m-%d %H:%M:%S')
    if record_seen_by_isc != "NA":
        record_seen_by_isc = record_seen_by_isc.strftime('%Y-%m-%d %H:%M:%S')
    record_seen_by_you = record_seen_by_you.strftime('%Y-%m-%d %H:%M:%S')
    db = sqlite3.connect(str(database_filename), timeout=15)
    cursor = db.cursor()
    print("Writing to database {} {} {} {} {} {}".format(database_filename, domain, record_seen_by_web, record_expires, record_seen_by_isc, record_seen_by_you))
    sql = "insert or replace into domains (domain, seen_by_web,expires,seen_by_isc,seen_by_you) values (?,?,?,?,?)"
    with database_lock:
            cursor.execute(sql, (domain, record_seen_by_web, record_expires, record_seen_by_isc, record_seen_by_you))
            db.commit()
    print(f"I pretended to update the record for {domain}")
    return 1


def retrieve(database_filename, domain):
    #Pass the timezone offset  hardcoded to utc for now
    #If record not found returns None,None,None,None
    #If record found rturns dates seen by web,expired,isc and you
    #If record is in database but domain registration expired it deletes the record and ignores it.
    timezone_offset = 0
    db = sqlite3.connect(str(database_filename), timeout=15)
    cursor = db.cursor()
    record = cursor.execute("select seen_by_web,expires, seen_by_isc, seen_by_you from domains where domain = ?" , (domain,) ).fetchone()
    if record:
        web,expires,isc,you = record
    else:
        print("No record in the database.  Returning None.")
        return (None,None,None,None)
    web = datetime.datetime.strptime(web, '%Y-%m-%d %H:%M:%S')
    expires = datetime.datetime.strptime(expires, '%Y-%m-%d %H:%M:%S')
    if expires < datetime.datetime.utcnow():
        print(f"Expired domain in database {domain} {expires}. Deleted")
        with database_lock:
            cursor.execute("delete from domains where domain=?", (domain,))
        return (None,None,None,None)
    if isc != "NA":
        isc = datetime.datetime.strptime(isc, '%Y-%m-%d %H:%M:%S')
    if you != "FIRST-CONTACT":
        you = datetime.datetime.strptime(you, '%Y-%m-%d %H:%M:%S')
    else:
        with database_lock:
            cursor.execute("update domains set seen_by_you=? where domain =?", ((datetime.datetime.utcnow()+datetime.timedelta(hours=timezone_offset)).strftime("%Y-%m-%d %H:%M:%S"), domain))
            db.commit()
    return (web,expires,isc,you)

def process_update_file(update_file,verify=True):
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
    return num_recs

def update_database(latest_version):
    print("Stubbed update database")
    return 0
    global config
    new_records_count = 0
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
        new_records_count += process_update_file(str(dst_path), verify=args.verify)
        config = update_config(database_version= version)
    return new_records_count 