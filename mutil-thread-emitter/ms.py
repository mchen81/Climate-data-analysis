#!/usr/bin/python
import sys
import socket  
import concurrent.futures
import gzip
import os

def read_gz_file(path, conn):
    with gzip.open(path, 'rb') as f:
        for line in f:
            conn.sendall(line)

nams = [ \
        #"/bigdata/mmalensek/nam/3hr/2015/", \
        #"/bigdata/mmalensek/nam/3hr/2016/", \
        "/bigdata/mmalensek/nam/3hr/2017/", \
        "/bigdata/mmalensek/nam/3hr/2018/", \
        "/bigdata/mmalensek/nam/3hr/2019/"]

if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    
    s = socket.socket()             # Create a socket object
    s.bind((host, port))            # Bind to the port
    s.listen(1)                     # Now wait for client connection.

    print('Server listening....')
    conn, addr = s.accept()     # Establish connection with client.
    print('connected by ' , addr)
    
    reading_futures = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=1) as executor:
        for nam_dir in nams:
            for root, dirs, files in os.walk(nam_dir):
                for filename in files:
                    print('Reading file: ' + nam_dir + filename)
                    fu = executor.submit(read_gz_file, nam_dir + filename, conn)
                    reading_futures.append(fu)
        
        print('Sending data to ' , addr)
        res = concurrent.futures.wait(reading_futures, timeout=120, return_when='ALL_COMPLETED')
        
        print('Failure process number: ' + str(len(res[1])))
        print('Program ends, connection is closed')
        conn.close()
