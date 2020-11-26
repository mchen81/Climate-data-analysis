## Usage
```
python3 ms.py [location] [host] [port]  
```
## Example
We want to send data from orion11/bigdata/nam ...   
So, the command would be: 
```
python3 ms.py /bigdata/mmalensek/nam/3hr/2018/ orion03 12888
```
This will open a socket on orion03(localhost):12888, once the socketTextStream from Spark is connected, it will send each row from 
.gz files under "/bigdata/mmalensek/nam/3hr/2018/".  
After all rows have been send, the connection will be closed.  
