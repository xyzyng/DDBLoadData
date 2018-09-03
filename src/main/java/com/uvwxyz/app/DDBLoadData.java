package com.uvwxyz.app;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.*;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

public class DDBLoadData {

    private String tableName;   // DDB table name
    private String primaryKey;  // primate key
    private String attrPrefix;  // attribute prefix
    private int numCol;         // number of columns
    private int numThread;      // number of threads to load data
    private String filePrefix = "/var/tmp/DDB-app/tempData";        // file to save data to disk, if enabled
    private String tempDir = "/var/tmp/DDB-app";                    // dir to save data to disk, if enabled
    private String seperator = ",";                                 // seprator used in file
    private long totalDataSize = 0L;                                // data size to be generated and loaded to DDB
    private boolean saveToDisk;                                     // flag to control save data to local disk
    private long totalSentDataSize = 0L;                            // total generated and loaded data
    private long totalSentItemsNum = 0L;                            // total generated and loaded items number


    private ArrayList<ddbPrepareDataJobRunnable> jobListPrepareData = new ArrayList<ddbPrepareDataJobRunnable>();  // job list for generate and load data

    // constructor
    public DDBLoadData(String tableName, String primaryKey, String attrPrefix, int numCol, int numThread, int numMB, boolean saveToDisk) {
        this.tableName = tableName;
        this.attrPrefix = attrPrefix;
        this.primaryKey = primaryKey;
        this.numCol = numCol;
        this.numThread = numThread;
        this.totalDataSize = 1024L * 1024L * numMB;
        this.saveToDisk = saveToDisk;

    }

    // random int number for customer id
    private String createRandomInt(int lowerRange, int upperRange) {
        Random random = new Random();
        return Integer.toString(random.nextInt(upperRange - lowerRange + 1) + lowerRange);
    }

    // calculate string length
    private int getStringSize(String s) {
        return s.getBytes().length;
    }

    // random double for product price
    private String createRandomDouble() {
        Random random = new Random();
        return Double.toString(random.nextDouble());
    }

    // calculate total file size, if save file to disk is enabled
    private long calcFileSize() {
        Path path = Paths.get(tempDir);
        long totalSize = 0;

        File tempDataDir = path.toFile();
        try {
            for (File file: tempDataDir.listFiles()) {
                Path tempDataFile = file.toPath();
                BasicFileAttributes attr = Files.readAttributes(tempDataFile, BasicFileAttributes.class);
                totalSize += attr.size();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return totalSize;
    }

    // create the thread to generate and load data. setup timer to monitor the progress of all threads
    public void startPrepareData() throws IOException {

        // if saveToDisk is enabled, create diretory for data file
        if (saveToDisk) {
            Path dir = Paths.get(tempDir);
            if (Files.notExists(dir)) {
                Files.createDirectory(dir);
            }
        }

        // start threads to create data file
        for(Integer i = 0; i < numThread; i++) {
           ddbPrepareDataJobRunnable ddbPrepareDataJob = new ddbPrepareDataJobRunnable(i);
           Thread ddbPrepareDataThread = new Thread(ddbPrepareDataJob);
           ddbPrepareDataThread.setName("PrepareDataTread " + i.toString());
           ddbPrepareDataThread.start();
           jobListPrepareData.add(ddbPrepareDataJob);
        }

        // timer to check check the progress of generating and loading data
        final Timer timer = new Timer();
        final long start = System.currentTimeMillis();

        timer.scheduleAtFixedRate(new TimerTask() {

            int timeElapsed = 0;

            public void run() {
                totalSentDataSize = 0L;
                totalSentItemsNum = 0L;
                timeElapsed += 1;

                // sum up sent data size and sent item numbers from each thread
                for (int i = 0 ; i < jobListPrepareData.size(); i++) {
                    ddbPrepareDataJobRunnable job = jobListPrepareData.get(i);
                    totalSentDataSize += job.getThreadSentDataSize();
                    totalSentItemsNum += job.getThreadSentItemsNum();
                }

                System.out.format("Elapsed time: %d seconds. Sent data size: %.2f MB. Sent items number: %d\n",timeElapsed, totalSentDataSize / 1024.0 / 1024.0, totalSentItemsNum);

                long tempFileSize = calcFileSize();
                if(saveToDisk) System.out.format("Temp data file total size: %.2f MB\n", tempFileSize / 1024.0 / 1024);

                // stop the thread when sent data size reaches total data size
                if (totalSentDataSize > totalDataSize) {
                    timer.cancel();
                    for (int i = 0 ; i < jobListPrepareData.size(); i++) {
                        ddbPrepareDataJobRunnable job = jobListPrepareData.get(i);
                        job.setRun(false);
                    }

                    // summary
                    long end = System.currentTimeMillis();
                    System.out.println("====================");
                    System.out.println("Took " + ((end - start)/1000) + " seconds to prepare data");
                    System.out.format("Total sent data size: %.2f MB\n", totalSentDataSize / 1024.0 / 1024.0);

                }
            }
        }, 0, 1000);
    }

    // thread task to prepare data and save in local disk
    class ddbPrepareDataJobRunnable implements Runnable {
        private String fileName;
        private boolean isRunnable = true;
        private int customerIDLowerRange;       // customer id lower range
        private int customerIDUpperRange;       // customer id higher range
        private long threadSentDataSize = 0L;   // process data size in this thread
        private long threadSentItemsNum = 0L;   // process items number in this thread
        private DynamoDB dynamoDB = null;
        final static int BATCH_WRITE_NUMBER = 24;   // DDB batch write size

        // swich to stop the thread
        public void setRun(boolean toSet) {
            isRunnable = false;
        }

        // constructor
        public ddbPrepareDataJobRunnable(int threadID) {
            this.fileName = filePrefix + Integer.toString(threadID);
            this.customerIDLowerRange = threadID * 10000000;
            this.customerIDUpperRange = this.customerIDLowerRange + 10000000;
            dynamoDB = new DynamoDB(Regions.AP_SOUTHEAST_2);
        }

        // getter of processed data size in this thread
        public long getThreadSentDataSize() {
            return threadSentDataSize;
        }

        // getter of process items number in this thread
        public long getThreadSentItemsNum() {
            return threadSentItemsNum;
        }

        public void run() {
            System.out.println("Prepare data thread started: " + Thread.currentThread().getName());

            BufferedWriter bw = null;

            try {
                if (saveToDisk) {
                    bw = new BufferedWriter(new FileWriter(new File(fileName)));
                }


                int batchCounter = BATCH_WRITE_NUMBER;
                TableWriteItems ddbAppTableWriteItems = null;
                int batchSize = 0;

                while(isRunnable) {
                    if (batchCounter == BATCH_WRITE_NUMBER) {       // initialize battch writes
                        ddbAppTableWriteItems = new TableWriteItems(tableName);
                        batchCounter--;
                        batchSize = 0;
                    } else if (batchCounter != 0) {                 // add each item to a batch writes task
                        Item item = new Item();
                        String customerID = createRandomInt(customerIDLowerRange, customerIDUpperRange);

                        if (saveToDisk) bw.write(customerID);

                        // calculate thred sent data size
                        threadSentDataSize += getStringSize(customerID);
                        batchSize += getStringSize(customerID);

                        // set item primary key
                        item.withPrimaryKey(primaryKey, customerID);

                        // set item attributes
                        for (int j = 1; j < numCol+1; j++) {
                            String price = createRandomDouble();
                            String colName = attrPrefix + Integer.toString(j);
                            item.withString(colName, price);

                            // calculate thread send data size
                            threadSentDataSize += getStringSize(price);
                            batchSize += getStringSize(price);

                            if (saveToDisk) {
                                bw.write(seperator);
                                bw.write(price);
                            }

                        }

                        ddbAppTableWriteItems.addItemToPut(item);

                        if (saveToDisk) bw.newLine();

                        batchCounter--;
                    }  else if (batchCounter == 0) {            // when all items added to batch write task, send all items to DDB
                        batchSize = 0;

                        // send batch items to DDB
                        BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(ddbAppTableWriteItems);

                        // handle unprossed items
                        do {

                            // Check for unprocessed keys which could happen if you exceed provisioned throughput
                            Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

                            if (outcome.getUnprocessedItems().size() != 0) {
                                outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
                            }

                        } while (outcome.getUnprocessedItems().size() > 0);

                        threadSentItemsNum += BATCH_WRITE_NUMBER;
                        batchCounter = BATCH_WRITE_NUMBER;
                    }
                }
            } catch (AmazonDynamoDBException e) {
                System.out.println(e.toString());
            } catch (Exception e ) {
                e.printStackTrace();
            }

            if (saveToDisk) {
                if (bw != null) {
                    try {
                        bw.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        }

    }

    public static void usage() {
        System.out.println("Parameters: -tableName <table_name> -primaryKey <primary_key> -attrPrefix <attribute_prefix> -numAttr <attribute_number> -numThread <thread_number> -numMB <data_size>");
        System.out.println("Or Parameters: -tableName <table_name> -primaryKey <primary_key> -attrPrefix <attribute_prefix> -numAttr <attribute_number> -numThread <thread_number> -numMB <data_size> -saveToDisk");
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 12 ||
            !args[0].equals("-tableName") ||
            !args[2].equals("-primaryKey") ||
            !args[4].equals("-attrPrefix") ||
            !args[6].equals("-numAttr") ||
            !args[8].equals("-numThread") ||
            !args[10].equals("-numMB")) {
                usage();
            }

        String tableName = args[1];
        String primaryKey = args[3];
        String attrPrefix = args[5];
        int numThread = Integer.parseInt(args[9]);
        int numCol = Integer.parseInt(args[7]);
        int numMB = Integer.parseInt(args[11]);
        boolean saveToDisk = false;

        if ((args.length == 13) && args[12].equals("-saveToDisk")) {
            saveToDisk = true;
        }

        DDBLoadData ddb = new DDBLoadData(tableName, primaryKey, attrPrefix, numCol, numThread, numMB, saveToDisk);

        ddb.startPrepareData();

    }
}
