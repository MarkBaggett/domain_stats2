import sqlite3
import threading
import datetime
import logging
import pathlib

log = logging.getLogger(__name__)
logfile = logging.FileHandler('domain_stats.log')
logformat = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
logfile.setFormatter(logformat)

  
class database_stats:
    def __init__(self, hit=0,miss=0, insert=0, delete=0) :
        self.hit = hit
        self.miss = miss
        self.insert = insert
        self.delete = delete

    def __repr__(self):
        return f"database_stats(hit={self.hit},miss={self.miss},insert={self.insert},delete={self.delete})"

class DomainStatsDatabase(object):

    def __init__(self, filename):
        self.filename = filename
        self.lock = threading.Lock()
        self.stats = database_stats()
        if not pathlib.Path(self.filename).exists():
            raise Exception(f"Database not found. {self.filename}")

    def update_record(self, domain, record_seen_by_web, record_expires, record_seen_by_isc, record_seen_by_you):
        record_seen_by_web = record_seen_by_web.strftime('%Y-%m-%d %H:%M:%S')
        record_expires = record_expires.strftime('%Y-%m-%d %H:%M:%S')
        if record_seen_by_isc != "NA":
            record_seen_by_isc = record_seen_by_isc.strftime('%Y-%m-%d %H:%M:%S')
        record_seen_by_you = record_seen_by_you.strftime('%Y-%m-%d %H:%M:%S')
        db = sqlite3.connect(self.filename, timeout=15)
        cursor = db.cursor()
        log.info("Writing to database {} {} {} {} {} {}".format(self.filename, domain, record_seen_by_web, record_expires, record_seen_by_isc, record_seen_by_you))
        sql = "insert or replace into domains (domain, seen_by_web,expires,seen_by_isc,seen_by_you) values (?,?,?,?,?)"
        with self.lock:
            cursor.execute(sql, (domain, record_seen_by_web, record_expires, record_seen_by_isc, record_seen_by_you))
            db.commit()
            self.stats.insert += 1
        return 1

    def delete_record(self, domain):
        db = sqlite3.connect(self.filename, timeout=15)
        cursor = db.cursor()
        with self.lock:
            cursor.execute("delete from domains where domain=?", (domain,))
            db.commit()
            self.stats.delete += 1
        return 1

    def get_record(self, domain):
        #Pass the timezone offset  hardcoded to utc for now
        #If record not found returns None,None,None,None
        #If record found rturns dates seen by web,expired,isc and you
        #If record is in database but domain registration expired it deletes the record and ignores it.
        timezone_offset = 0
        db = sqlite3.connect(self.filename, timeout=15)
        cursor = db.cursor()
        record = cursor.execute("select seen_by_web,expires, seen_by_isc, seen_by_you from domains where domain = ?" , (domain,) ).fetchone()
        if record:
            web,expires,isc,you = record
        else:
            self.stats.miss += 1
            log.info("No record in the database.  Returning None.")
            return (None,None,None,None)
        web = datetime.datetime.strptime(web, '%Y-%m-%d %H:%M:%S')
        expires = datetime.datetime.strptime(expires, '%Y-%m-%d %H:%M:%S')
        if expires < datetime.datetime.utcnow():
            log.info(f"Expired domain in database {domain} {expires}. Deleted")
            with self.lock:
                cursor.execute("delete from domains where domain=?", (domain,))
                db.commit()
                self.stats.delete += 1
            return (None,None,None,None)
        if isc != "NA":
            isc = datetime.datetime.strptime(isc, '%Y-%m-%d %H:%M:%S')
        if you != "FIRST-CONTACT":
            you = datetime.datetime.strptime(you, '%Y-%m-%d %H:%M:%S')
        else:
            with self.lock:
                cursor.execute("update domains set seen_by_you=? where domain =?", ((datetime.datetime.utcnow()+datetime.timedelta(hours=timezone_offset)).strftime("%Y-%m-%d %H:%M:%S"), domain))
                db.commit()
        self.stats.hit += 1
        return (web,expires,isc,you)

    def process_update_file(self, update_file):
        """ Process csv in the format command, domain, web, expire, seen_by_isc """
        """ if command is + we add the record setting if it doesnt already exist"""
        """ if command is - we delete the record"""
        if not pathlib.Path(update_file).exists():
            log.info(f"The specified update file {update_file} does not exists.")
            return 0
        new_domains = open(update_file).readlines()
        num_recs = len(new_domains)
        db = sqlite3.connect(self.filename, timeout=15)
        cursor = db.cursor()    
        for pos,entry in enumerate(new_domains):
            if pos % 50 == 0:
                print("\r|{0:-<50}| {1:3.2f}%".format("X"*( 50 * pos//num_recs), 100*pos/num_recs),end="")
            command, domain, web, expires, isc = entry.strip().split(",")
            domain = reduce_domain(domain)
            if command == "+":
                record = cursor.execute("select seen_by_web, expires, seen_by_isc, seen_by_you from domains where domain = ?" , (domain,) ).fetchone()
                if not record:
                    self.update_record(domain, web, expires, isc, "FIRST-CONTACT")
                    log.debug(f"Record added to database for domain {domain}")
                else:
                    log.debug(f"Record already exists skipped {domain}")
            elif command == "-":
                self.delete_record(domain)
                log.debug(f"Deleted record for {domain}")
        db.commit()
        print("\r|XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX| 100.00% FINISHED")
        return num_recs

    def update_database(self, latest_version, config):
        new_records_count = 0
        latest_major, latest_minor = map(int, str(latest_version).split("."))
        current_major, current_minor = map(int, str(config['database_version']).split("."))
        log.info(f"Updating from {current_version} to {latest_version}")
        if latest_major > current_major:
            log.info("WARNING: Domain Stats database is a major revision behind. Database required rebuild.")
            raise Exception("WARNING: Domain Stats database is a major revision behind. Database required rebuild.")
        target_updates = range(current_minor+1, latest_minor+1 )
        for update in target_updates:
            version = f"{current_major}.{update}"
            log.info(f"Now applying update {version}")
            tgt_url = f"{config['target_updates']}/{current_major}/{update}.txt" 
            dst_path = pathlib.Path().cwd() / "data" / f"{current_major}" / f"{update}.txt"
            urllib.request.urlretrieve(tgt_url, str(dst_path))
            new_records_count += process_update_file(str(dst_path))
        return latest_version, new_record_count