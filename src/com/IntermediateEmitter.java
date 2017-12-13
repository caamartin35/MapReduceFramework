package com;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import com.util.KeyValuePair;
import com.util.WorkerStorage;

class IntermediateEmitter implements Emitter{
	private final String workerName;
	private static final String fileName = "intermediate.txt";
	private final String filePath;
	private PrintWriter writer;
	
	public IntermediateEmitter(String workerName){
		this.workerName = workerName;
		filePath = WorkerStorage.getIntermediateResultsDirectory(this.workerName) + "/" + fileName;
		File f = new File(filePath);
		if (f.exists())
			f.delete();
		
		try {
			writer = new PrintWriter(new BufferedWriter(new FileWriter(this.filePath, true)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void emit(String key, String value) throws IOException {
		KeyValuePair pair = new KeyValuePair(key, value);
		
		writer.println(pair.toString());
		
		writer.flush();
		
	}
	
	public void close(){
		writer.close();
	}
	public static String getFileName(){
		return fileName;
	}
	
}