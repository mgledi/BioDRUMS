package com.unister.semweb.drum.tutorial;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.io.FileUtils;

import com.unister.semweb.herv.HERV;
import com.unister.semweb.sdrum.GlobalParameters;
import com.unister.semweb.sdrum.api.FileStorageException;
import com.unister.semweb.sdrum.api.SDRUM;
import com.unister.semweb.sdrum.api.SDRUM_API;
import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;

/**
 * Example implementation for showing how to write to the drumSDB.
 * 
 * @author n.thieme
 * 
 */
public class WriteTutorial {
    /** The property file for the drumSDB. */
    private static final String sdrumPropertyFile = "sdrum.properties";

    /** The filename of the range hash function. */
    private static final String hashFunctionFilename = "rangeHashFunction.txt";

    /** Random generator that is used to generate test data. */
    private static final Random randomGenerator = new Random(0);

    public static void main(String[] args) {
        WriteTutorial tutorium = new WriteTutorial();
        tutorium.addSomeEntries();
    }

    /** Demonstrates how to add entries to the drumSDB. */
    public HERV[] addSomeEntries() {
        // First we need the parameters for the drumSDB, for example the directory of the database.
        GlobalParameters<HERV> globalParameters = new GlobalParameters<HERV>(
                sdrumPropertyFile, new HERV());
        initialise(globalParameters);

        // The we need the RangeHashFunction that is read from a file.
        RangeHashFunction hashFunction = null;
        try {
            hashFunction = new RangeHashFunction(new File(hashFunctionFilename));
        } catch (IOException ex) {
            throw new RuntimeException("Range hash function file could not be loaded. File was. "
                    + hashFunctionFilename, ex);
        }

        // After that we need the drumSDB and open the database.
        SDRUM<HERV> sdrum = null;

        try {
            sdrum = SDRUM_API.createOrOpenTable(hashFunction, globalParameters);
        } catch (IOException ex) {
            throw new RuntimeException("Could not initialise the sdrum.", ex);
        }

        // We generate some test data.
        HERV[] testdata = generateTestdata(1000);

        // And we write these data to the drumSDB. Important: close the drumSDB after all work is finished. That causes
        // the drumSDB to synchronises all data remaining in the memory buckets to disk.
        try {
            sdrum.insertOrMerge(testdata);
            sdrum.close();
        } catch (FileStorageException ex) {
            throw new RuntimeException("Could not instert the data into sdrum.", ex);
        } catch (InterruptedException ex) {
            throw new RuntimeException("Sdrum was interrupted while adding elements.", ex);
        }
        return testdata;
    }

    /** Generates the given number of test data. The keys and the values are randomly generated. */
    public HERV[] generateTestdata(int numberToGenerate) {
        HERV[] result = new HERV[numberToGenerate];
        for (int i = 0; i < result.length; i++) {
            result[i] = new HERV();
            result[i].key = generateRandomByteArray(HERV.keySize);
            result[i].value = generateRandomByteArray(HERV.byteBufferSize
                    - HERV.keySize);
        }
        return result;
    }

    /** Generates a byte array with the given size and fills the bytes with random data.. */
    private byte[] generateRandomByteArray(int numberOfDigits) {
        byte[] result = new byte[numberOfDigits];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) randomGenerator.nextInt();
        }
        return result;
    }

    /** Initialises the tutorial environment by deleting the database directory first. */
    public void initialise(GlobalParameters<HERV> globalParameters) {
        FileUtils.deleteQuietly(new File(globalParameters.databaseDirectory));
    }

}
