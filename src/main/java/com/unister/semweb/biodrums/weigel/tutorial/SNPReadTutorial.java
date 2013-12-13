package com.unister.semweb.biodrums.weigel.tutorial;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.unister.semweb.biodrums.weigel.SNP;
import com.unister.semweb.drums.DRUMSParameterSet;
import com.unister.semweb.drums.api.DRUMS;
import com.unister.semweb.drums.api.DRUMS.AccessMode;
import com.unister.semweb.drums.api.DRUMSException;
import com.unister.semweb.drums.api.DRUMSInstantiator;
import com.unister.semweb.drums.api.DRUMSIterator;
import com.unister.semweb.drums.api.DRUMSReader;
import com.unister.semweb.drums.file.FileLockException;

/**
 * This class provides a extensively documented example how to read {@link SNP}-records from a DRUMS-table. You should
 * run the provided {@link SNPWriteTutorial} first, to have some data. <br>
 * 
 * @author Martin Nettling
 * 
 */
public class SNPReadTutorial {

    /**
     * This method starts the ReadTutorial. The location of your DRUMS is defined in the property file in the
     * "./src/main/resources/SNPExample/". Please see the parameter <b>DATABASE_DIRECTORY</b>.
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
         * needed Generic to SNP.
         */
        DRUMSParameterSet<SNP> globalParameters = new DRUMSParameterSet<SNP>(new File("/tmp/sdrumDatabase"));

        /**
         * Use the {@link DRUMSInstantiator}-class to open the DRUMS-table.
         */
        DRUMS<SNP> drums = DRUMSInstantiator.openTable(AccessMode.READ_ONLY, globalParameters);

        /**
         * ############################# Single Select Example
         * You can define several elements, which should be selected. As you can see, only the key of each {@link SNP}
         * defined.
         */
        System.out.println("\n\n############## Perform single select test on three elements ##############");
        SNP toSearch1 = new SNP((byte) 1, 7192175, (char) 50);
        SNP toSearch2 = new SNP((byte) 3, 15717, (char) 50);
        SNP toSearch3 = new SNP((byte) 5, 30931, (char) 50);
        List<SNP> select = drums.select(toSearch1.getKey(), toSearch2.getKey(), toSearch3.getKey());
        System.out.println("SNPs read by single selects.");
        for (SNP snp : select) {
            System.out.println(snp);
        }

        /**
         * ############################# Iterator Example {@link DRUMS} allows you to instantiate {@link DRUMSIterator}
         * s. A {@link DRUMSIterator} needs only a few
         * kilobytes of memory. Further, only
         * one file at once is opened for reading.
         */
        System.out.println("\n\n############## Read first 100 elements with an DRUMSIterator ##############");
        DRUMSIterator<SNP> iterator = drums.getIterator();
        int k = 0;
        while (iterator.hasNext() && k++ < 100) {
            System.out.println("Element " + k + " read by iterator: " + iterator.next());
        }
        iterator.close();

        /**
         * To perform range selects you can use the iterator or the method {@link DRUMSReader#getRange(byte[],byte[]}.
         * You can get a {@link DRUMSReader} by calling {@link DRUMS#getReader()}
         */
        System.out.println("\n\n############## Read Range on Chromosome 1 with DRUMSReader ##############");
        DRUMSReader<SNP> reader = drums.getReader();
        SNP lowerKey = new SNP((byte) 1, 100000, (char) 50);
        SNP upperKey = new SNP((byte) 1, 200000, (char) 50);
        List<SNP> range = reader.getRange(lowerKey.getKey(), upperKey.getKey());
        System.out.println("Found " + range.size() + " SNPs between " + lowerKey + " and " + upperKey);
        reader.closeFiles();

        // filter data
        System.out
                .println("\n\n############## Filter data in previous read range. Allow only mutations to 'A' ##############");
        Iterator<SNP> rangeIterator = range.iterator();
        ArrayList<SNP> filteredRange = new ArrayList<SNP>();
        while (rangeIterator.hasNext()) {
            SNP snp = rangeIterator.next();
            if (snp.getTo() == (byte) 'A') {
                filteredRange.add(snp);
            }
        }
        System.out.println("Found " + filteredRange.size() + " SNPs between " + lowerKey + " and " + upperKey
                + " , which mutated to 'A'");

        drums.close();
    }
}