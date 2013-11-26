package com.unister.semweb.herv;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Martin Nettling
 */
public class HERVParser {
    private static final Logger log = LoggerFactory.getLogger(HERVParser.class);

    /** Counter for all lines */
    private long overallLines;

    /** Counter for lines which are causing an error */
    private long errorLines;

    private BufferedReader bufferedReader;

    /**
     * Instantiates a new parser for HERV-data.
     * 
     * @param filename
     *            the name of the file, which contains the HERV-Data
     * @param bufferSize
     *            the size of the buffer to use
     * @throws IOException
     */
    public HERVParser(String filename, int bufferSize) throws IOException {
        FileReader fileReader = new FileReader(filename);
        bufferedReader = new BufferedReader(fileReader, bufferSize);
    }

    /**
     * This method to read the next correct line from the underlying file. It parses this line and instantiates a new
     * {@link HERV} object.
     * 
     * @return the next {@link HERV}-object. NULL if no next object can be read.
     * @throws IOException
     */
    public HERV readNext() throws IOException {
        String nextLine = bufferedReader.readLine();
        HERV actualObject = null;
        // read lines until a new HERV-object could be read or the end of the file is reached
        while (nextLine != null && actualObject == null) {
            actualObject = parseLine(nextLine);
            overallLines++;
            if (actualObject != null) {
                break;
            }
            errorLines++;
            nextLine = bufferedReader.readLine();
        }
        return actualObject;
    }

    /**
     * This method parses the given line and generates a new {@link HERV}-object.
     * 
     * @param line
     *            the line to parse.
     * @return the {@link HERV}-object presented by the given line.
     */
    public HERV parseLine(String line) {
        try {
            HERV newData = new HERV();
            String[] Aline = line.split("\t");

            String idHERVString = Aline[0];
            int idHERVConverter = Integer.valueOf(idHERVString);
            char idHERV = (char) idHERVConverter;

            String chromosomeString = Aline[1];
            byte chromosome = (byte) extractChromosom(chromosomeString);
            if (chromosome < 0) {
                log.debug("Could not parse the chromosome sequence number: {}", line);
                return null;
            }

            String startHERVString = Aline[6];
            int startHERVConverted = Integer.valueOf(startHERVString);
            char startHERV = (char) startHERVConverted;

            String endHERVString = Aline[7];
            int endHERVConverted = Integer.valueOf(endHERVString);
            char endHERV = (char) endHERVConverted;

            String startChromosomeString = Aline[8];
            int startChromosome = Integer.valueOf(startChromosomeString);

            String endChromosomeString = Aline[9];
            int endChromosome = Integer.valueOf(endChromosomeString);

            String eValueString = Aline[10];
            double eValue = Double.valueOf(eValueString);

            byte strandOnChromosome = (byte) 1;
            if (startChromosome > endChromosome) {
                strandOnChromosome = (byte) 0;
            }

            newData.setKey(chromosome, startChromosome, endChromosome, startHERV, endHERV, idHERV);
            newData.setStrandOnChromosome(strandOnChromosome);
            newData.setEValue(eValue);

            return newData;
        } catch (Exception ex) {
            log.debug("Could not parse one line. Error message: {}; Line: {}", ex.getMessage(), line);
            return null;
        }
    }

    private byte extractChromosom(String chromosomString) {
        String value = chromosomString.substring(3).trim().toLowerCase();
        // handle x and y chromosome
        if (value.equals("x")) {
            return 23;
        } else if (value.equals("y")) {
            return 24;
        }

        // handle chromsome 1 to 22.
        try {
            byte result = Byte.valueOf(value);
            return result;
        } catch (NumberFormatException ex) {
            log.error("Sequence number in the wrong format: {}; extracted number: {}", chromosomString, value);
            return -1;
        }
    }

    /** @return the number of read lines */
    public long getOverallLines() {
        return overallLines;
    }

    /** @return the number of read lines with an error */
    public long getErrorLines() {
        return errorLines;
    }
}
