package com;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.util.KeyValuePair;
import com.util.Log;
import com.util.WorkerStorage;

/**
 * This class includes an ExecuteShuffle class which is 
 * sent to every other worker server so that each worker
 * will send its disjoint set of data to this worker server.
 * @author Cody
 *
 */
class ExecuteReduceTaskCommand extends WorkerCommand{
	private static final long serialVersionUID = -7835038571174344304L;
	private static final int MAX_POOL_SIZE = Runtime.getRuntime().availableProcessors();
	private final ReduceTask reduceTask;
	private final List<WorkerInfo> mapWorkers;
	private final WorkerInfo reduceWorker;
	private final int numReducers;
	
	public ExecuteReduceTaskCommand(ReduceTask reduceTask, int numReducers, List<WorkerInfo> mapWorkers, WorkerInfo worker){
		this.numReducers = numReducers;
		this.reduceTask = reduceTask;
		this.mapWorkers = mapWorkers;
		this.reduceWorker = worker;
	}
	
	@Override
	public void run() {
		System.out.println("Executing Reduce Task");
		Socket socket = getSocket();
		int numThreads = Math.min(MAX_POOL_SIZE, mapWorkers.size());
		ExecutorService mExecutor = Executors.newFixedThreadPool(numThreads);
		FinalEmitter finalEmitter =  new FinalEmitter(this.reduceWorker.getName());
		List<List<KeyValuePair>> resultsList = new ArrayList<List<KeyValuePair>>();
		
		try {
			List<Future<List<KeyValuePair>>> results = null;
			List<CommandCallable> callables = new ArrayList<CommandCallable>();
			List<String> values;
			
			//send the executeshuffle command to every map worker.
			for (WorkerInfo worker : mapWorkers){
				ExecuteShuffleCommand command = new ExecuteShuffleCommand(worker, this.reduceWorker, this.numReducers);
				callables.add(new CommandCallable(command, worker));
			}
			
			//send shuffle command to all map workers to receive
			//intermediate key value pairs from them
			try {
				results = mExecutor.invokeAll(callables);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			
			
			//load the results list
			for (Future<List<KeyValuePair>> future : results){
				resultsList.add(future.get());
			}
			
			KeyValuePair pair;
			
			//get all values associated with a single key, then
			//send that key and those values off to the task executer.
			while((pair = getFirstPair(resultsList)) != null){
				values = getAllValues(pair, resultsList);
			
				//execute the reduce task on the values with the given key.
				this.reduceTask.execute(pair.getKey(), values.iterator(), finalEmitter);
			}
			
			ObjectOutputStream outstream = new ObjectOutputStream(socket.getOutputStream());
			outstream.writeObject(null);
			outstream.close();
		
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			System.out.println("Done Reduce Task");
			finalEmitter.close();
			mExecutor.shutdown();
		}
		
	}
	
	/**
	 * 
	 * @param pair - key value pair that holds key to get all values associated with
	 * @param resultsList - the total results list.
	 * @return
	 */
	private List<String> getAllValues(KeyValuePair pair, List<List<KeyValuePair>> resultsList) {
		List<String> values = new ArrayList<String>();
		
		for (List<KeyValuePair> list : resultsList){
			for (int i=0; i<list.size(); i++){
				KeyValuePair curPair = list.get(i);
				if (pair.getKey().equals(curPair.getKey())){
					values.add(curPair.getValue());
					list.remove(i);
					i--;
				}
			}
		}
		return values;
	}

	/**
	 * 
	 * @param resultsList - list to get the first key value pair from
	 * @return the first key value pair found in the list of lists
	 */
	private KeyValuePair getFirstPair(List<List<KeyValuePair>> resultsList) {
		for (List<KeyValuePair> list : resultsList){
			if (!list.isEmpty()){
				return list.get(0);
			}
		}
		
		return null;
	}

	/**
	 * class that is a command for reduce workers to send to map workers
	 * to receive the disjoint data from each other worker.
	 * @author Cody
	 *
	 */
	private class ExecuteShuffleCommand extends WorkerCommand{
		private static final long serialVersionUID = 5321442485747262481L;
		private final WorkerInfo mapWorker;
		private final int numReducers;
		private final WorkerInfo reduceWorker;
		private static final String TAG = "ExecuteShuffleCommand";
		
		public ExecuteShuffleCommand(WorkerInfo mapWorker, WorkerInfo reduceWorker, int numReducers){
			this.numReducers = numReducers;
			this.mapWorker = mapWorker;
			this.reduceWorker = reduceWorker;
		}
		
		@Override
		public void run() {
			// Get the socket to use to send results back to the client. Note that
	        // the WorkerServer will close this socket for us, so we don't need to
	        // worry about that.
	        Socket socket = getSocket();
	        File file = null;
	        try {
	            // Opens a FileInputStream for the specified file, execute the task,
	            // and close the input stream once we've calculated the result.
	            file = new File(WorkerStorage.getIntermediateResultsDirectory(this.mapWorker.getName()) + "/intermediate.txt");
	           
	            BufferedReader br = new BufferedReader(new FileReader(file));
				String line;
				KeyValuePair curPair;
				List<KeyValuePair> pairs = new ArrayList<KeyValuePair>();
				
				while ((line = br.readLine()) != null) {
					curPair = parseLine(line);
					String reducerHash = Integer.toString(Math.abs(curPair.getKey().hashCode() % numReducers) + 1);
					String reducerNumber = reduceWorker.getName().substring(reduceWorker.getName().length()-1);
					if (reducerHash.equals(reducerNumber)){
						pairs.add(curPair);
					}
				}
				br.close();

	            // Open an ObjectOutputStream to use to communicate with the client
	            // that sent this command, and write the result back to the client.
	            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
	            out.writeObject(pairs);
	        } catch (IOException e) {
	            Log.e(TAG, "I/O error while executing command.", e);
	        }
			
		}
		
		private KeyValuePair parseLine(String line) {
			String key = null, value = null;
			Pattern p = Pattern.compile("=(.*),");
			Matcher m = p.matcher(line);
			if (m.find())
				key = m.group(0).substring(1, m.group(0).length()-1);
			p = Pattern.compile("value=(.*)>");
			m = p.matcher(line);
			if (m.find()){
				value = m.group(0).substring(6,  m.group(0).length()-1);
			}
			return new KeyValuePair(key, value);
		}
		
	}
}