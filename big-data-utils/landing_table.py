import subprocess
import logging
import argparse
import time
from impala.dbapi import connect
import traceback

"""
Script landing table
- (optional) pause/restart processus during landing table processing
- Copy data from landing table to historic table
- Remove/re-create landing table
- (optional) re-insert period data in landing table

$ python landing_table -h

@last-update: 2019-03-27 
@author: mbouchet
"""

# Config logger
logging.basicConfig()
logger = logging.getLogger('landing_table')


def check_pid_exist(pid):
    """
    Check on the system if the PID exist
    :param str/int pid: processus id
    :raises Exception: Raise exception if the PID doesn't exist
    """
    logger.debug('check_pid_exist(%s)' % pid)
    p = subprocess.Popen('ps %s' % pid, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.communicate()
    if p.returncode != 0:
        raise Exception('Error, the PID %s doesn\'t exist on the system' % pid)


def pause_pid(pid):
    """
    Pause a processus (send signal)
    :param str/int pid: processus id
    :raises Exception: Raise exception if error during command
    """
    logger.debug('pause_pid(%s)' % pid)
    p = subprocess.Popen('kill -STOP %s' % pid, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sdtout, sdterr = p.communicate()
    if p.returncode != 0:
        raise Exception('Error during stopping process: %s' % sdterr)


def restart_pid(pid):
    """
    Restart paused processus (send signal)
    :param str/int pid: processus id
    :raises Exception: Raise exception if error during command
    """
    logger.debug('restart_pid(%s)' % pid)
    p = subprocess.Popen('kill -CONT %s' % pid, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sdtout, sdterr = p.communicate()
    if p.returncode != 0:
        raise Exception('Error during restarting process: %s' % sdterr)


def kudu_landing_table(kudu_histo, kudu_landing, keep_data_landing, req_filter):
    """
    Copy data from landing table to historic table, re-create landing table
    :param str kudu_histo: kudu historic table name
    :param str kudu_landing: kudu landing table name
    :param bool keep_data_landing: Re-insert some data in landing table after drop, based on req_filter param
    :param str req_filter: if keep_data_landing is True, insert data from landing table tmp to landing table, with req_filter in WHERE clause
    """
    logger.debug('kudu_landing_table(%s, %s, %s, %s)' % (kudu_histo, kudu_landing, keep_data_landing, req_filter))
    bdd = connect(host='X', port=21050, auth_mechanism='GSSAPI', use_ssl=False, ca_cert=None, ldap_user=None, ldap_password=None, kerberos_service_name='impala')
    cursor = bdd.cursor()
    kudu_landing_tmp = '%s_tmp' % kudu_landing
    # Create a copy of landing table
    cursor.execute('SHOW CREATE TABLE %s' % kudu_landing)
    req_create_landing = cursor.fetchone()[0]
    req_create_landing_tmp = req_create_landing.replace(kudu_landing, kudu_landing_tmp)
    cursor.execute(req_create_landing_tmp)
    # Copy data from landing table to landing table tmp
    cursor.execute('INSERT INTO TABLE %s SELECT * FROM %s' % (kudu_landing_tmp, kudu_landing))
    # Drop landing table
    cursor.execute('DROP TABLE %s;' % kudu_landing)
    # Recreate empty landing table
    cursor.execute(req_create_landing)
    # Copy data from landing table tmp to historic table
    cursor.execute('INSERT INTO TABLE %s select * from %s;' % (kudu_histo, kudu_landing_tmp))
    # Copy data last day to the landing table
    if keep_data_landing:
        cursor.execute('INSERT INTO TABLE %s select * from %s WHERE %s;' % (kudu_landing, kudu_landing_tmp, req_filter))
    # Drop temporary landing table
    cursor.execute('DROP TABLE %s;' % (kudu_landing_tmp))



if __name__ == '__main__':

    try:
        # Parse arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("kudu_table", help="Kudu table name (ex: kudu_db.table_1)", type=str)
        parser.add_argument("kudu_landing_table", help="Kudu lading table name (ex: kudu_db.table_1_landing)", type=str)
        parser.add_argument("--pid_list", help="List of PID (processus id) to stop during landing table processing (ex: --p=1 or --p=1,2)", type=str)
        parser.add_argument("--keep_data_landing", help="Use with --filter option, keep data on landing table based on filter condition", action="store_true")
        parser.add_argument("--filter", help="Use with --keep-data-landing option, condition WHERE in select insert request (ex : --filter='time_extract >= date_add(now(),-1)'", type=str)
        parser.add_argument("--unsafe", help="Disabled security that process only landing table with the string 'landing' inside the name", action="store_true")
        parser.add_argument("--debug", help="Enabled debug logs", action="store_true")
        args = parser.parse_args()

        # Log level
        if args.debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.WARNING)

        # Pause PID
        if args.pid_list:
            pid_list = args.pid_list.split(',')
            for pid in pid_list:
                check_pid_exist(pid)
                pause_pid(pid)

        # Controls kudu table name
        if not args.unsafe:
            if 'landing' not in args.kudu_landing_table:
                raise Exception('Error: The landing table name doesn\'t contain the "landing" string, disabled this control with option --unsafe')

        kudu_landing_table(args.kudu_table, args.kudu_landing_table, args.keep_data_landing, args.filter)

        # Restart PID
        if args.pid_list:
            pid_list = args.pid_list.split(',')
            for pid in pid_list:
                restart_pid(pid)

    except Exception as e:
        traceback.print_exc()
        logger.error(e)
        exit(1)
    
