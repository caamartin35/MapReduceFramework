package com;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.util.KeyValuePair;
import com.util.Log;
import com.util.StaffUtils;

/**
 * This class represents the "master server" in the distributed map/reduce
 * framework. The {@link MasterServer} is in charge of managing the entire
 * map/reduce computation from beginning to end. The {@link MasterServer}
 * listens for incoming client connections on a distinct host/port address, and
 * is passed an array of {@link WorkerInfo} objects when it is first initialized
 * that provides it with necessary information about each of the available
 * workers in the system (i.e. each worker's name, host address, port number,
 * and the set of {@link Partition}s it stores). A single map/reduce computation
 * managed by the {@link MasterServer} will typically behave as follows:
 *
 * <ol>
 * <li>Wait for the client to submit a map/reduce task.</li>
 * <li>Distribute the {@link MapTask} across a set of "map-workers" and wait for
 * all map-workers to complete.</li>
 * <li>Distribute the {@link ReduceTask} across a set of "reduce-workers" and
 * wait for all reduce-workers to complete.</li>
 * <li>Write the final key/value pair results of the computation back to the
 * client.</li>
 * </ol>
 */
public class MasterServer extends Thread {
	private static final String TAG = "MasterServer";
	private static final int CLIENT_POOL_SIZE = 1;
    private final int mPort;
    private final List<WorkerInfo> mWorkers;
    private final ExecutorService clientExecutor;
    
    /**
     * The {@link MasterServer} constructor.
     *
     * @param masterPort The port to listen on.
     * @param workers Information about each of the available workers in the
     *        system.
     */
    public MasterServer(int masterPort, List<WorkerInfo> workers) {
        mPort = masterPort;
        mWorkers = workers;
        clientExecutor = Executors.newFixedThreadPool(CLIENT_POOL_SIZE);
    }

    @Override
    public void run() {
    	try {
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(mPort);
            } catch (IOException e) {
                Log.e(TAG, "Could not open server socket on port " + mPort + ".", e);
                return;
            }

            Log.i(TAG, "Listening for incoming commands on port " + mPort + ".");

            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    // Handle the client's request on a background thread, and
                    // immediately begin listening for other incoming client
                    // connections once again.
                    clientExecutor.execute(new ClientRequestHandler(clientSocket, mWorkers));
                } catch (IOException e) {
                    Log.e(TAG, "Error while listening for incoming connections.", e);
                    break;
                }
            }

            Log.i(TAG, "Shutting down...");

            try {
                serverSocket.close();
            } catch (IOException e) {
                // Ignore because we're about to exit anyway.
            }
        } finally {
            clientExecutor.shutdown();
        }
    }
    
    private static class ClientRequestHandler implements Runnable {
    	private static final int MAX_POOL_SIZE = Runtime.getRuntime().availableProcessors();
        private final Socket mSocket;
        private final List<WorkerInfo> workers;
        private final ExecutorService mExecutor;
        private boolean mapOK, reduceOK;
        
        public ClientRequestHandler(Socket socket, List<WorkerInfo> workers) {
        	int numThreads = Math.min(MAX_POOL_SIZE, workers.size());
            this.mSocket = socket;
            this.workers = workers;
            this.mExecutor = Executors.newFixedThreadPool(numThreads);
            this.mapOK = false;
            this.reduceOK = false;
        }

        @Override
        public void run() {
			try {
				ObjectInputStream in = new ObjectInputStream(mSocket.getInputStream());
				MapTask mapTask = (MapTask) in.readObject();
	        	ReduceTask reduceTask = (ReduceTask) in.readObject();
	        	List<CommandCallable> callables = new ArrayList<CommandCallable>();
	        	List<Future<List<KeyValuePair>>> results;
	        	List<WorkerInfo> currentMapWorkers = new ArrayList<WorkerInfo>();
	        	List<WorkerInfo> currentReduceWorkers = new ArrayList<WorkerInfo>();
	        	
	        	for (WorkerInfo worker : workers)
	        		currentMapWorkers.add(worker);
	        	
	        	
	        	while (!mapOK){
	        		this.mapOK = true;
	        		
		        	System.out.println("Sending Map Task");
		        	
		        	callables = setMapCallables(mapTask, currentMapWorkers);
		        	results = mExecutor.invokeAll(callables);
		        	
		        	int i = 0;
		        	for (Future<List<KeyValuePair>> result : results){
		        		try {
		        			result.get();
		        			i++;
						} catch (ExecutionException e) {
							currentMapWorkers.remove(i);
							mapOK = false;
							callables.clear();
						}
		        	}
		        	
		        	System.out.println("Finished Executing Map Task");
	        	
	        	}
		        	
	        	callables.clear();
	        	
	        	for (WorkerInfo worker : workers)
	        		currentReduceWorkers.add(worker);
	        	
	        	while (!reduceOK){
	        		this.reduceOK = true;
	        		
		        	System.out.println("Sending Reduce Task");
		        	
		        	callables = setReduceCallables(reduceTask, currentReduceWorkers, currentMapWorkers);
		        	results = mExecutor.invokeAll(callables);
	        	
		        	int i = 0;
		        	for (Future<List<KeyValuePair>> result : results){
		        		try {
							result.get();
							i++;
						} catch (ExecutionException e) {
							currentReduceWorkers.remove(i);
							reduceOK = false;
							callables.clear();
						}
		        	}
	        	
		        	System.out.println("Finished Executing Reduce Task");
		        	
	        	}
	        	
	        	System.out.println("Sending Result to Client.");
	        	
	        	Result result = new Result(currentReduceWorkers, FinalEmitter.getFileName());
	        	
	        	ObjectOutputStream out = new ObjectOutputStream(mSocket.getOutputStream());
	        	
	        	out.writeObject(result);
	        	
	        	in.close();
	        	out.close();
	        	
	        	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }

		private List<CommandCallable> setReduceCallables(ReduceTask reduceTask, List<WorkerInfo> workers, List<WorkerInfo> mapWorkers) {
			List<CommandCallable> ret = new ArrayList<CommandCallable>();
			ExecuteReduceTaskCommand command;
			
			for (WorkerInfo worker : workers){
				command = new ExecuteReduceTaskCommand(reduceTask, workers.size(), mapWorkers, worker);
				ret.add(new CommandCallable(command, worker));
			}
			return ret;
		}

		private List<CommandCallable> setMapCallables(MapTask mapTask, List<WorkerInfo> workers) {
			Map<String, List<String>> map = new HashMap<String, List<String>>();
			Map<String, String> finalMap = new HashMap<String, String>();
			List<Partition> partitions = new ArrayList<Partition>();
			List<String> parWorkers;
			List<CommandCallable> ret = new ArrayList<CommandCallable>();
			ExecuteMapTaskCommand command;
			
			//get all partitions in a list
			for (WorkerInfo worker : workers){
				for (int i=0;i<worker.getPartitions().size();i++){
					partitions.add(worker.getPartitions().get(i));
				}
			}
			
			//map each partition to a list of workers who have access to it
			while (!partitions.isEmpty()){
				Partition partition = new Partition(partitions.get(0).getPartitionName(), partitions.get(0).getWorkerName());	
				parWorkers = new ArrayList<String>();
				
				for (int j=0; j<partitions.size(); j++){
					if (partition.getPartitionName().equals(partitions.get(j).getPartitionName())){
						parWorkers.add(partitions.get(j).getWorkerName());
						partitions.remove(j);
						j--;
					}	
				}
			
				map.put(partition.getPartitionName(), parWorkers);
			}
			
			//choose a worker for each partition
			Iterator<String> iterator = map.keySet().iterator();
			
			while (iterator.hasNext()){
				String current = iterator.next();
				int index = (int) (Math.random()*(map.get(current).size()));
				finalMap.put(current, map.get(current).get(index));
			}
			
			iterator = finalMap.keySet().iterator();
			
			
			for (WorkerInfo worker : workers){
				partitions = new ArrayList<Partition>();
				iterator = finalMap.keySet().iterator();
				while (iterator.hasNext()){
					String curPartition = iterator.next();
					String curWorker = finalMap.get(curPartition);
					
					if (curWorker.equals(worker.getName())){
						System.out.println("Adding Partition: " + curPartition + " For Worker: " + curWorker);
						partitions.add(new Partition(curPartition, curWorker));
					}
				}
				command =  new ExecuteMapTaskCommand(mapTask, worker, partitions);
				ret.add(new CommandCallable(command, worker));
			}
			
			return ret;
		}
    }
    
    /********************************************************************/
    /***************** STAFF CODE BELOW. DO NOT MODIFY. *****************/
    /********************************************************************/

    /**
     * Starts the master server on a distinct port. Information about each
     * available worker in the distributed system is parsed and passed as an
     * argument to the {@link MasterServer} constructor. This information can be
     * either specified via command line arguments or via system properties
     * specified in the <code>master.properties</code> and
     * <code>workers.properties</code> file (if no command line arguments are
     * specified).
     */
    public static void main(String[] args) {
        StaffUtils.makeMasterServer(args).start();
    }

}
