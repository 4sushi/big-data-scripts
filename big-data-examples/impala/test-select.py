# -*- coding: utf-8 -*-

from impala.dbapi import connect

"""
Script to test select requests
"""

def test1(conn, cursor):
    cursor.execute("SELECT * FROM `kudu`.x")
    result = cursor.fetchall()
    if len(result) != 20000000:
        raise Exception('Bad result length test1')

def test2(conn, cursor):
    cursor.execute("SELECT * FROM `kudu`.x WHERE id = '100000'")
    result = cursor.fetchall()
    if len(result) != 1:
        raise Exception('Bad result length test2')

def test3(conn, cursor):
    cursor.execute("SELECT * FROM `kudu`.x WHERE tweet = 'xxx'")
    result = cursor.fetchall()
    if len(result) != 1:
        raise Exception('Bad result length test3')

def test4(conn, cursor):
    cursor.execute("SELECT * FROM `kudu`.x WHERE date_tweet >= '2017-01-01' AND date_tweet <= '2017-12-31'")
    result = cursor.fetchall()
    if len(result) != 1052076:
        raise Exception('Bad result length test4')

def test5(conn, cursor):
    cursor.execute("SELECT * FROM `kudu`.x WHERE rate >= 1 AND rate <= 3")
    result = cursor.fetchall()
    if len(result) != 9998846:
        raise Exception('Bad result length test5')

def test6(conn, cursor):
    cursor.execute("SELECT count(1) as nb FROM `kudu`.x")
    result = cursor.fetchone()
    if result[0] != 20000000:
        raise Exception('Bad result length test6')


if __name__ == '__main__':
    impala_port=21050
    bdd = connect(host="X", port=impala_port, auth_mechanism='GSSAPI', use_ssl=False, ca_cert=None, ldap_user=None, ldap_password=None, kerberos_service_name='impala')
    cursor = bdd.cursor()

    for i in range(0, 1):
        test1(bdd, cursor)
        test2(bdd, cursor)
        test3(bdd, cursor)
        test4(bdd, cursor)
        test5(bdd, cursor)
        test6(bdd, cursor)
