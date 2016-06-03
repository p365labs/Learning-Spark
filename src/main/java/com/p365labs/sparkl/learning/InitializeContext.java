package com.p365labs.sparkl.learning;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * Created by federicopanini on 03/06/16.
 *
 * Example on how to create a SparkContext
 */
public class InitializeContext {

    private SparkContext sc;

    /**
     * in the constructor there's an example on how to create the SparkContext
     * which is necessary for start using SPARK, and create RRD.
     */
    public InitializeContext() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("testspark");
        SparkContext sc = new SparkContext(conf);

        this.sc = sc;
    }

    public SparkContext getContext() {
        return this.sc;
    }
}
