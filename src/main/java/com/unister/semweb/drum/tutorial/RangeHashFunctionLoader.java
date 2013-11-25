package com.unister.semweb.drum.tutorial;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import com.unister.semweb.drums.bucket.hashfunction.RangeHashFunction;

public class RangeHashFunctionLoader {
    private static final String rangeHashFunctionFilename = "rangeHashFunction.txt";
    private static final String tmpFile = "rangeHashFunction.txt";

    public static RangeHashFunction loadRangeHashFunction() throws IOException {
        InputStream fileStream = RangeHashFunctionLoader.class.getResourceAsStream(rangeHashFunctionFilename);
        FileWriter tmpHashFile = new FileWriter(tmpFile);
        IOUtils.copy(fileStream, tmpHashFile);

        fileStream.close();
        tmpHashFile.close();
        RangeHashFunction rangeHashFunction = new RangeHashFunction(new File(tmpFile));
        return rangeHashFunction;
    }
}
