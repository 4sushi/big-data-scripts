# -*- coding: utf-8 -*
import hdfs
from hdfs.ext.kerberos import KerberosClient
import logging
import os
import json
import time
import signal
import sys
import datetime
import zlib
import shutil
from http.client import IncompleteRead as Http_incompleteRead
from urllib3.exceptions import IncompleteRead as Urllib3_incompleteRead
from threading import Event
from multiprocessing import Process, active_children
import multiprocessing

"""
author: mbouchet
last update: 2019-04-10
"""

ZLIB_COMPRESSION_AVG = 7.8 # ZLIB compression average based on small test, with files of 128mb with tweets inside
MAX_FILE_SIZE = (1024 * 1024 * 128) * ZLIB_COMPRESSION_AVG  # 128mb (in bytes) * ZLIB_COMPRESSION_AVG 


class WriteHdfs:
    """
    Write content in HDFS in mutilple files (like blocks), compress content with ZLIB
    """

    def __init__(self, hdfs_urls, path_hdfs='./', max_file_size=MAX_FILE_SIZE, max_process=4, log_level='INFO'):
        """
        :param hdfs_url list[str]: hdfs url (ex: ['X'])
        :param path_hdfs str: path to write file in HDFS
        :param max_file_size int: limit size before create a new file and save the current file to hdfs (compressed)
        :param max_process int: number of subprocess to compress and write file in HDFS (max_process > 0)
        :param log_level str: logger level
        """
        # Config logger
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self.logger = logging.getLogger('WriteHdfs')
        self.logger.addHandler(stream_handler)
        self.logger.setLevel(log_level)
        # Config signal exit
        signal.signal(signal.SIGINT, self.__signal_handler)
        # Try to find the active namenode in the list
        for hdfs_url in hdfs_urls:
            try:
                hdfs_client = KerberosClient(hdfs_url)
                hdfs_client.list(path_hdfs)
                self.hdfs_url = hdfs_url
                self.logger.info('identify namenode: %s' % hdfs_url)
                break
            except hdfs.util.HdfsError:
                continue
        self.path_hdfs = path_hdfs
        self.max_process = max_process
        # Files settings
        self.file_size = 0
        self.file_name = self.__generate_file_name()
        self.max_file_size = max_file_size
        

    def __del__(self):
        self.stop()


    def write_content(self, content):
        """
        Write content in temporary file
        When the size of the file exceeds the max, compress the file and store it in HDFS
        :param content str: content to write in file
        """
        content_size = len(content)

        if content_size > self.max_file_size:
            raise ValueError('The size of the content is superior to the maximum file size: %s > %s' % (content_size, self.max_file_size))

        if self.file_size + content_size > self.max_file_size :
            # If too much processus, wait
            while(len(active_children()) >= self.max_process):
                self.logger.debug('Too much processus, wait ended one process...')
                time.sleep(1) 
            self.logger.debug('New process for file: %s' % self.file_name)
            p = Process(target=WriteHdfs.__write_to_hdfs, args=(self.hdfs_url, self.path_hdfs, self.file_name, self.logger))
            p.start()
            # Reset file info
            self.file_size = 0
            self.file_name = self.__generate_file_name()
   
        with open(self.file_name, 'a+') as f:
            f.write(content)
            self.file_size += content_size

    
    def stop(self):
        self.logger.info('Stop WriteHdfs')
        # Write current file (even is not full)
        if self.file_size > 0:
            WriteHdfs.__write_to_hdfs(self.hdfs_url, self.path_hdfs, self.file_name, self.logger)
        # Wait end sub process
        while len(active_children()) > 0:
            time.sleep(1)
            self.logger.info('Wait end all processus, please wait...')
        self.logger.info('Done')


    def __generate_file_name(self):
        """
        Generate a file name based on the current time
        """
        return '%s.tmp' % int(round(time.time() * 1000)) 


    @staticmethod
    def __write_to_hdfs(hdfs_url, path_hdfs, file_name, logger):
        """
        - Compress local file with ZLIB
        - Put compressed file on HDFS
        - Remove local files
        :param path_hdfs str: 
        :param file_name str: 
        """
        logger.debug('Start process __write_to_hdfs for file: %s' % file_name)
        # Compress file
        file_name_zlib = '%s.gz' % file_name
        with open(file_name, 'rb') as f_in:
            with open(file_name_zlib, 'wb') as f_out:
                f_out.write(zlib.compress(f_in.read()))

        # Write file to HDFS
        try:
            hdfs_client = KerberosClient(hdfs_url)
        except hdfs.util.HdfsError as e:
            logger.error('Error during HDFS connection, wait...: %s' % e)
            time.sleep(10)
            WriteHdfs.__write_to_hdfs(hdfs_url, path_hdfs, file_name, logger)
            return

        file_name_hdfs = file_name_zlib.replace('.tmp', '')
        file_path_hdfs = '%s/%s' % (path_hdfs, file_name_hdfs)
        try:
            hdfs_client.upload(file_path_hdfs, file_name_zlib)
        except hdfs.util.HdfsError as e:
            logger.error('Error during HDFS write, wait...: %s' % e)
            time.sleep(10)
            WriteHdfs.__write_to_hdfs(hdfs_url, path_hdfs, file_name, logger)
            return

        # Remove tmp files
        os.remove(file_name)
        os.remove(file_name_zlib)
        logger.debug('End process __write_to_hdfs for file: %s' % file_name)


    def __signal_handler(self, sig, frame):
        """
        Capture exist signal
        """
        # Is not main process
        if '_MainProcess' not in str(type(multiprocessing.current_process())) :
            return
        self.logger.info('Signal to exist')
        self.stop()
        sys.exit(0)


if __name__ == '__main__':
    """
    Simple example
    """
    write_hdf = WriteHdfs(['http://X.X.X.X:50070', 'http://X.X.X.X:50070'], log_level='DEBUG')
    for i in range(1, 1000000):
        print(time.time())
        random_content = 'a' * 1024 * 1024 * 30 * i + '\n'
        write_hdf.write_content(random_content)
        time.sleep(0.5)

