#!/usr/bin/python
import sys
import socket  
import concurrent.futures
import gzip
import os

def read_gz_file(path, conn):
    with gzip.open(path, 'rb') as f:
        for line in f:
            conn.send(line)


if __name__ == "__main__":
    dir_prefix = sys.argv[1]
    host = sys.argv[2]
    port = int(sys.argv[3])
    
    s = socket.socket()             # Create a socket object
    s.bind((host, port))            # Bind to the port
    s.listen(5)                     # Now wait for client connection.

    print('Server listening....')
    conn, addr = s.accept()     # Establish connection with client.
    print('connected')
    
    reading_futures = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
        for root, dirs, files in os.walk(dir_prefix):
            for filename in files:
                print('executing: ' + dir_prefix+filename)
                fu = executor.submit(read_gz_file, dir_prefix+filename, conn)
                reading_futures.append(fu)
        
        print('Running...')
        res = concurrent.futures.wait(reading_futures, timeout=120, return_when='ALL_COMPLETED')
        
        print(res)
        print('Program ends, connection closed')
        conn.close()

