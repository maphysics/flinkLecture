package com.lecture;

import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Created by Melissa on 5/24/2017.
 */
public class Main {

    public static void main(String[] args) throws Exception{
        IngestData id = new IngestData();
        id.getTokenizedNews().print();
    }
}
