package com;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.Callable;

import com.util.KeyValuePair;


class CommandCallable implements Callable<List<KeyValuePair>> {
    private final WorkerCommand mCommand;
    private final WorkerInfo mWorker;

    public CommandCallable(WorkerCommand command, WorkerInfo worker) {
        mCommand = command;
        mWorker = worker;
    }

    /**
     * Returns the {@link WorkerConfig} object that provides information
     * about the worker that this callable task is responsible for
     * interacting with.
     */
    public WorkerInfo getWorker() {
        return mWorker;
    }

    @SuppressWarnings("unchecked")
	@Override
    public List<KeyValuePair> call() throws Exception {
        Socket socket = null;
        try {
            // Establish a connection with the worker server.
            socket = new Socket(mWorker.getHost(), mWorker.getPort());

            // Create the ObjectOutputStream and write the WorkerCommand
            // over the network to be read and executed by a WorkerServer.
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(mCommand);
           
            // Note that we instantiate the ObjectInputStream AFTER writing
            // the object over the objectOutputStream. Initializing it
            // immediately after initializing the ObjectOutputStream (but
            // before writing the object) will cause the entire program to
            // block, as described in this StackOverflow answer:
            // http://stackoverflow.com/q/5658089/844882:
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            // Read and return the worker's final result.
            return (List<KeyValuePair>) in.readObject();
        } finally {
            try {
                if (socket != null) {
                    socket.close();
                }
            } catch (IOException e) {
                // Ignore because we're about to exit anyway.
            }
        }
    }
}
