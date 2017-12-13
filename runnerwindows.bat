::- This script will start 4 workers, a master server, and a client, each 
::- as its own separate process. This script will likely come in handy as
::- you test your code.
::- You must run this script from your hw6 project's root directory.
::
echo "Starting workers in their own processes..."
START java -Dfile.encoding=UTF-8 -classpath bin com.WorkerServer 15214
START java -Dfile.encoding=UTF-8 -classpath bin com.WorkerServer 15215
START java -Dfile.encoding=UTF-8 -classpath bin com.WorkerServer 15216
START java -Dfile.encoding=UTF-8 -classpath bin com.WorkerServer 15217

::- Sleep for 5 seconds to let workers start.
ping 192.0.2.2 -n 1 -w 5000 > nul
echo "Starting master server..."
START java -Dfile.encoding=UTF-8 -classpath bin com.MasterServer 15218 worker1 127.0.0.1 15214 1,4,5,7,9 worker2 127.0.0.1 15215 1,3,6,8,9 worker3 127.0.0.1 15216 2,3,6,8,10 worker4 127.0.0.1 15217 2,4,5,7,10

::- Sleep for 2 seconds to let master server start.
ping 192.0.2.2 -n 1 -w 2000 > nul
echo "Starting client..."
java -Dfile.encoding=UTF-8 -classpath bin com.plugin.wordprefix.WordPrefixClient 127.0.0.1 15218
