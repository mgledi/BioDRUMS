package com.unister.semweb.drum.tutorial;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.api.DRUMS;
import com.unister.semweb.drums.api.DRUMSException;
import com.unister.semweb.drums.api.DRUMSInstantiator;
import com.unister.semweb.drums.api.DRUMSReader;
import com.unister.semweb.drums.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.drums.file.FileLockException;
import com.unister.semweb.herv.HERV;

/**
 * Example implementation that reads in previous written data from drumSDB.
 * 
 * @author n.thieme
 * 
 */
public class ReadTutorial {
    /** The name of the property file. */
    private static final String sdrumPropertyFile = "sdrum.properties";

    /** The name of the range hash function file. */
    private static final String hashFunctionFilename = "rangeHashFunction.txt";

    /** The drumSDB to use. */
    private DRUMS<HERV> sdrum;

    /** The reader instance to use. */
    private DRUMSReader<HERV> reader;

    public static void main(String[] args) {
        ReadTutorial tutorial = new ReadTutorial();
        List<byte[]> writtenKeys = tutorial.writeTestdata();

        tutorial.initialise();
        tutorial.readData(writtenKeys);
        tutorial.cleanUp();
    }

    /** Initialises the drumSDB and the reader. */
    public void initialise() {
        // First we need the parameters for the drumSDB (where are the database located, ...)
        GlobalParameters<HERV> globalParameters = new GlobalParameters<HERV>(sdrumPropertyFile, new HERV());

        // Then we need to load the range hash function.
        RangeHashFunction hashFunction = null;
        try {
            hashFunction = new RangeHashFunction(new File(hashFunctionFilename));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        // We must open the drumSDB.
        try {
            sdrum = DRUMSInstantiator.createOrOpenTable(hashFunction, globalParameters);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        // Finally we get the reader from the drum.
        reader = null;
        try {
            reader = sdrum.getReader();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } catch (FileLockException ex) {
            throw new RuntimeException(ex);
        }
    }

    /** Takes a list of keys those HERV data will be read and print out all the HERV data. */
    public void readData(List<byte[]> keys) {
        for (byte[] oneKey : keys) {
            HERV readElement = readOneEntry(oneKey);
            StringBuilder builder = new StringBuilder();
            builder.append("HERV Date: ");
            builder.append("Key: ").append(Arrays.toString(readElement.key)).append("; ");
            builder.append("Start at chromosome position: ").append(readElement.getStartPositionChromosome())
                    .append("; ");
            builder.append("End position at chromosome: ").append(readElement.getEndPositionChromosome()).append("; ");
            builder.append("Start HERV position: ").append(Integer.valueOf(readElement.getStartHERV())).append("; ");
            builder.append("End HERV position: ").append(Integer.valueOf(readElement.getEndHERV()));
            System.out.println(builder.toString());
        }
    }

    /** Closes the reader and the drumSDB. */
    public void cleanUp() {
        reader.closeFiles();

        try {
            sdrum.close();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    /** Reads the HERV data associated with the given <code>key</code> from the drumSDB. */
    public HERV readOneEntry(byte[] key) {
        List<HERV> data = null;
        try {
            data = reader.get(key);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } catch (DRUMSException ex) {
            throw new RuntimeException(ex);
        }

        int readDataSize = data.size();
        if (readDataSize == 0) {
            return null;
        } else if (readDataSize == 1) {
            return data.get(0);
        } else {
            throw new RuntimeException("There exists more than one data under the same key!");
        }
    }

    /**
     * Reads the HERV data that corresponds the given list of keys in one bunch. This method is faster than reading one
     * entry after another..
     */
    public List<HERV> readBunchOfData(List<byte[]> toRead) {
        List<HERV> data = null;
        try {
            data = reader.get(toRead.toArray(new byte[0][0]));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } catch (DRUMSException ex) {
            throw new RuntimeException(ex);
        }
        return data;
    }

    /** It is used to build an initial database that we can read. It returns the keys of all written data. */
    public List<byte[]> writeTestdata() {
        WriteTutorial writer = new WriteTutorial();
        HERV[] writtenData = writer.addSomeEntries();

        List<byte[]> writtenKeys = new ArrayList<byte[]>();
        for (HERV oneDate : writtenData) {
            writtenKeys.add(oneDate.key);
        }
        return writtenKeys;
    }
}