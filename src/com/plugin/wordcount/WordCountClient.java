package com.plugin.wordcount;

import com.util.StaffUtils;
import com.AbstractClient;

/**
 * A client in the map/reduce framework that submits and executes a word count
 * task.
 *
 * DO NOT MODIFY THIS CLASS.
 */
public class WordCountClient extends AbstractClient {

    public WordCountClient(String masterHost, int masterPort) {
        super(masterHost, masterPort);
    }

    @Override
    protected WordCountMapTask getMapTask() {
        return new WordCountMapTask();
    }

    @Override
    protected WordCountReduceTask getReduceTask() {
        return new WordCountReduceTask();
    }

    /**
     * Run this class to submit and execute a word count task to the framework.
     */
    public static void main(String[] args) {
        StaffUtils.makeWordCountClient(args).execute();
    }

}
