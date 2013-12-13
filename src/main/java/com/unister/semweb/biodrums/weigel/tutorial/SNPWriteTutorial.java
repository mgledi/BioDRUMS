package com.unister.semweb.biodrums.weigel.tutorial;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;

import com.unister.semweb.biodrums.weigel.FilteredVariantParser;
import com.unister.semweb.biodrums.weigel.SNP;
import com.unister.semweb.drums.DRUMSParameterSet;
import com.unister.semweb.drums.api.DRUMS;
import com.unister.semweb.drums.api.DRUMSException;
import com.unister.semweb.drums.api.DRUMSInstantiator;
import com.unister.semweb.drums.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.drums.storable.AbstractKVStorable;

/**
 * This class provides a extensively documented example how to create a new DRUMS-table and how to write SNP-records
 * into this table.<br>
 * <br>
 * First you have to extend the {@link AbstractKVStorable} class. You must implement your efficient representation of
 * your data. We already implemented a representation for SNP-data. Please have a look at the {@link SNP}-class.
 * 
 * @author Martin Nettling
 * 
 */
public class SNPWriteTutorial {
    /**
     * This method starts the WriteTutorial. The location of your DRUMS is defined in the property file in the
     * "./src/main/resources/SNPExample/". Please see the parameter <b>DATABASE_DIRECTORY</b>.
     * 
     * @param args
     *            no arguments are needed
     * @throws IOException
     * @throws InterruptedException
     * @throws DRUMSException
     */
    public static void main(String[] args) throws IOException, DRUMSException, InterruptedException {
        /**
         * First the parameters for the DRUMS-table must be instantiated and loaded. You can define a property file,
         * from which all parameters are loaded. Further, the type of data must be defined. This is done by setting the
         * needed Generic to SNP.
         */
        DRUMSParameterSet<SNP> globalParameters = new DRUMSParameterSet<SNP>("SNPExample/drums.properties", new SNP());
        /** to repeat the test we have to delete the table first */
        FileUtils.deleteQuietly(new File(globalParameters.DATABASE_DIRECTORY));

        /**
         * {@link DRUMS} needs a consistent hash function. The {@link SNP} class provides a method to generate a
         * {@link RangeHashFunction} for Arabidopsis thaliana.
         */
        RangeHashFunction hashFunction = SNP.createHashFunction();

        /**
         * The {@link DRUMSInstantiator}-class provides several factory methods to instantiate a DRUMS-table. The table
         * does not exists. It must be created before you can insert data.
         */
        DRUMS<SNP> drums = DRUMSInstantiator.createTable(hashFunction, globalParameters);
        
        // obtain the concrete path to the example file
        URL url = SNPWriteTutorial.class.getClassLoader().getResource("SNPExample/SHA_filtered_variant.txt.gz");
        /**
         * To load all {@link SNP}s from a raw file, you can use the provided parser. You have to provide an ID for the
         * corresponding ecotype.
         */
        FilteredVariantParser parser = new FilteredVariantParser(url.getFile(), 50);

        /**
         * Add all {@link SNP}s to your {@link DRUMS}-instance.
         */
        SNP snp;
        while ((snp = parser.readNext()) != null) {
            drums.insertOrMerge(snp);
        }
        /**
         * Don't forget to close your {@link DRUMS}-instance.
         */
        drums.close();
    }
}
