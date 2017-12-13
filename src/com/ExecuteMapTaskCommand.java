package com;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class ExecuteMapTaskCommand extends WorkerCommand{
	private static final long serialVersionUID = -7835038571174344304L;
	private final MapTask mapTask;
	private final WorkerInfo worker;
	private final List<Partition> partitions;
	
	
	public ExecuteMapTaskCommand(MapTask mapTask, WorkerInfo worker, List<Partition> partitions){
		this.mapTask = mapTask;
		this.worker = worker;
		this.partitions = partitions;
	}
	
	@Override
	public void run() {
		Socket socket = getSocket();
		
		System.out.println("Executing the map task.");
		List<Iterator<File>> files = new ArrayList<Iterator<File>>();
		for (int i=0; i<partitions.size(); i++){
			files.add(partitions.get(i).iterator());
		}
		IntermediateEmitter intermediateEmitter = new IntermediateEmitter(this.worker.getName());
		FileInputStream filestream = null;
		
		for (int i=0; i<files.size();i++){
			while (files.get(i).hasNext()){
				try {
					
					filestream = new FileInputStream(files.get(i).next());
					this.mapTask.execute(filestream, intermediateEmitter);
					
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					filestream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		intermediateEmitter.close();
		
		try {
			ObjectOutputStream outstream = new ObjectOutputStream(socket.getOutputStream());
			outstream.writeObject(null);
			outstream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	
		System.out.println("Done Map Task.");
	}
	
}