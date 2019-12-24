import sqlite3
import threading
import datetime

database_lock = threading.Lock()

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


def retrieve(database_filename, domain):
    #Pass the timezone offset  hardcoded to utc for now
    timezone_offset = 0
    db = sqlite3.connect(str(database_filename), timeout=15)
    cursor = db.cursor()
    record = cursor.execute("select seen_by_web,expires, seen_by_isc, seen_by_you from domains where domain = ?" , (domain,) ).fetchone()
    if record:
        web,expires,isc,you = record
    else:
        print("returning nones")
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