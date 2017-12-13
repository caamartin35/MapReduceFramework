package com;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

class Result implements Serializable{
	private static final long serialVersionUID = -2307369320408824074L;
	
	private List<WorkerInfo> workers;
	private String resultFileName;
	
	public Result(List<WorkerInfo> workers, String resultFileName){
		this.workers = workers;
		this.resultFileName = resultFileName;
	}
	
	private List<String> getFinalFilePaths(){
		List<String> filePaths = new ArrayList<String>();
		
		for (WorkerInfo worker : workers){
			filePaths.add("worker_storage/" + worker.getName() + "/final_results/" + resultFileName);
		}
		
		return filePaths;
	}
	
	public List<String> getResultsLocations(){
		List<String> locations = new ArrayList<String>();
		List<String> filePaths = getFinalFilePaths();
		int i = 0;
		
		for (WorkerInfo worker : workers){
			locations.add("File: " + filePaths.get(i) + " Host: " + worker.getHost() + " Port: " + worker.getPort());
			i++;
		}
		
		return locations;
	}
}