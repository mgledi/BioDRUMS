package com.unister.semweb.biodrums.herv.tutorial;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.unister.semweb.biodrums.herv.HERV;
import com.unister.semweb.drums.DRUMSParameterSet;
import com.unister.semweb.drums.api.DRUMS;
import com.unister.semweb.drums.api.DRUMS.AccessMode;
import com.unister.semweb.drums.api.DRUMSException;
import com.unister.semweb.drums.api.DRUMSInstantiator;
import com.unister.semweb.drums.api.DRUMSIterator;
import com.unister.semweb.drums.api.DRUMSReader;
import com.unister.semweb.drums.file.FileLockException;

/**
 * This class provides a extensively documented example how to read {@link HERV}-records from a DRUMS-table. You should
 * run the provided {@link HERVWriteTutorial} first, to have some data. <br>
 * 
 * @author Martin Nettling
 * 
 */
public class HERVReadTutorial {

    /**
     * This method starts the ReadTutorial. The location of your DRUMS is defined in the property file in the
     * "./src/main/resources/HERVExample/". Please see the parameter <b>DATABASE_DIRECTORY</b>.
     * 
     * @param args
     *            no arguments are needed
     * @throws IOException
     * @throws DRUMSException
     * @throws InterruptedException
     * @throws FileLockException
     */

    public static void main(String[] args) throws IOException, DRUMSException, InterruptedException, FileLockException {
        /**
         * First the parameters for the DRUMS-table must be instantiated and loaded. You can define a property file,
         * from which all parameters are loaded. Further, the type of data must be defined. This is done by setting the
         * needed Generic to HERV.
         */
        DRUMSParameterSet<HERV> globalParameters = new DRUMSParameterSet<HERV>(new File("/tmp/sdrumDatabase"));

        /**
         * Use the {@link DRUMSInstantiator}-class to open the DRUMS-table.
         */
        DRUMS<HERV> drums = DRUMSInstantiator.openTable(AccessMode.READ_ONLY, globalParameters);

        /**
         * ############################# Single Select Example
         * You can define several elements, which should be selected. As you can see, only the key of each {@link HERV}
         * defined.
         */
        System.out.println("\n\n############## Perform single select test on three elements ##############");
        HERV toSearch1 = new HERV((byte) 2, 218021470, 218021226, (char) 2628, (char) 2874, (char) 44889);
        HERV toSearch2 = new HERV((byte) 9, 44224152, 44224431, (char) 2591, (char) 2874, (char) 44889);
        HERV toSearch3 = new HERV((byte) 7, 36422198, 36422431, (char) 2631, (char) 2864, (char) 44889);
        List<HERV> select = drums.select(toSearch1.getKey(), toSearch2.getKey(), toSearch3.getKey());
        System.out.println("HERVs read by single selects.");
        for (HERV herv : select) {
            System.out.println(herv);
        }

        /**
         * ############################# Iterator Example {@link DRUMS} allows you to instantiate {@link DRUMSIterator}
         * s. A {@link DRUMSIterator} needs only a few
         * kilobytes of memory. Further, only
         * one file at once is opened for reading.
         */
        System.out.println("\n\n############## Read first 100 elements with an DRUMSIterator ##############");
        DRUMSIterator<HERV> iterator = drums.getIterator();
        int k = 0;
        while (iterator.hasNext() && k++ < 100) {
            System.out.println("Element " + k + " read by iterator: " + iterator.next());
        }
        iterator.close();

        /**
         * To perform range selects you can use the iterator or the method {@link DRUMSReader#getRange(byte[],byte[]}.
         * You can get a {@link DRUMSReader} by calling {@link DRUMS#getReader()}.<br>
         * <br>
         * This request corresponds to: SELECT all HERVs, that can be found on chromosome 7
         */
        System.out.println("\n\n############## Read Range on Chromosome 7 with DRUMSReader ##############");
        DRUMSReader<HERV> reader = drums.getReader();
        HERV lowerKey = new HERV((byte) 7, 100000, 0, (char) 0, (char) 0, (char) 0);
        HERV upperKey = new HERV((byte) 7, 2000000, 0, (char) 0, (char) 0, (char) 0);
        List<HERV> range = reader.getRange(lowerKey.getKey(), upperKey.getKey());
        System.out.println("Found " + range.size() + " HERVs between " + lowerKey + " and " + upperKey);
        reader.closeFiles();

        // filter data
        System.out
                .println("\n\n############## Filter data in previous read range. " +
                        "\n############## Allow only HERVs with an E-Value smaller than 1e-50 ##############");
        Iterator<HERV> rangeIterator = range.iterator();
        ArrayList<HERV> filteredRange = new ArrayList<HERV>();
        while (rangeIterator.hasNext()) {
            HERV herv = rangeIterator.next();
            if (herv.getEValue() < 1e-50) {
                filteredRange.add(herv);
            }
        }
        System.out.println("Found " + filteredRange.size() + " HERVs between " + lowerKey + " and " + upperKey
                + " , with an E-Value smaller than 1e-50");

        drums.close();
    }
}