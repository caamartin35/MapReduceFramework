package com.plugin.wordprefix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.Emitter;
import com.ReduceTask;

/**
 * The reduce task for a word-prefix map/reduce computation.
 */
public class WordPrefixReduceTask implements ReduceTask {
    private static final long serialVersionUID = 6763871961687287020L;

    @Override
    public void execute(String key, Iterator<String> values, Emitter emitter) throws IOException {
        int sum = 0;
        int maxSum = 0;
        String curValue, maxValue = null;
        List<String> valList = new ArrayList<String>();
        
        //load values into a list
        while (values.hasNext()){
        	valList.add(values.next());
        }
        
        //while the list is not empty, we count how many of one value
        //exist, then set it to the max if it is, and remove all of those
        //from the list
        while (!valList.isEmpty()){
        	sum = 0;
        	curValue = new String(valList.get(0));
        	
        	for (int i=0; i<valList.size(); i++){
        		if (curValue.equals(valList.get(i))){
        			sum++;
        			valList.remove(i);
        			i--;
        		}	
        	}
        	
        	if (sum > maxSum){
        		maxValue = curValue;
        		maxSum = sum;
        	}
        }
        
        emitter.emit(key, maxValue);
    }

}
