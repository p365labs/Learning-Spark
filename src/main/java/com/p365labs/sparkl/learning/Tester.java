package com.p365labs.sparkl.learning;

import org.apache.spark.SparkContext;

/**
 * Created by federicopanini on 03/06/16.
 */
public class Tester {

    public static void main(String[] args) throws Exception {
        Tester tester = new Tester();

        int command = 0;
        if (args.length != 0) {
            command = Integer.valueOf(args[0]);
        }

        switch (command) {
            case 1:
                tester.testContext();
                break;
            case 2:
                tester.testReadFile();
                break;
            default :
                tester.showHelp();;
                break;
        }
    }

    public void testContext() {
        InitializeContext ic = new InitializeContext();

        SparkContext sc = ic.getContext();

        System.out.println("This is a new created / initialized SparkContext : " + sc);
    }

    public void testReadFile() {
        WordCount wc = new WordCount();
        wc.doWordCound(
                "file:////Users/federicopanini/projects/spark_learning/README.md",
                "file:////Users/federicopanini/projects/spark_learning/input_counted.csv"
        );
    }

    public void showHelp() {
        System.out.println("Start Using Spark small examples available : ");
        System.out.println("use command as the first argument to invoke the main method");
        System.out.println();
        System.out.println("use 1 for : example on how to initialize SparkContext");
    }
}
