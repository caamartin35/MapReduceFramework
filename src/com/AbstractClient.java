package com;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;

import com.plugin.wordcount.WordCountClient;
import com.plugin.wordprefix.WordPrefixClient;

/**
 * An abstract client class used primarily for code reuse between the
 * {@link WordCountClient} and {@link WordPrefixClient}.
 */
public abstract class AbstractClient {
    private final String mMasterHost;
    private final int mMasterPort;

    /**
     * The {@link AbstractClient} constructor.
     *
     * @param masterHost The host name of the {@link MasterServer}.
     * @param masterPort The port that the {@link MasterServer} is listening on.
     */
    public AbstractClient(String masterHost, int masterPort) {
        mMasterHost = masterHost;
        mMasterPort = masterPort;
    }

    protected abstract MapTask getMapTask();

    protected abstract ReduceTask getReduceTask();

    public void execute() {
        final MapTask mapTask = getMapTask();
        final ReduceTask reduceTask = getReduceTask();
        Socket sock;
        ObjectOutputStream outstream;
        ObjectInputStream instream;
        Result finalResult;
        
        // TODO: Submit the map/reduce task to the master and wait for the task
        // to complete.
        
        try {
        	System.out.println("Creating Socket on port: " + mMasterPort);
        	//create the socket
        	sock = new Socket(mMasterHost, mMasterPort);
        	
        	System.out.println("Socket Connected");
        	//create the object output stream
			outstream = new ObjectOutputStream(sock.getOutputStream());
			
			//send the tasks
			outstream.writeObject(mapTask);
			outstream.writeObject(reduceTask);
			
			//create the object input stream
			instream = new ObjectInputStream(sock.getInputStream());
			
			//read in the final result
			finalResult = (Result) instream.readObject();
			
			List<String> locations = finalResult.getResultsLocations();
			
			System.out.println("Results found at:");
			for (String location : locations){
				System.out.println(location);
			}
			
			outstream.close();
			instream.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
    }

}
