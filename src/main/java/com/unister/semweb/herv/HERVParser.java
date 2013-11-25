package com.unister.semweb.herv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HERVParser {
    private static final Logger log = LoggerFactory.getLogger(HERVParser.class);
    private long overallLines;
    private long errorLines;

    private BufferedReader bufferedReader;
    String[] lines;
    int actLine;

    public HERVParser(String filename, int bufferSize) throws IOException {
        actLine = 0;
        FileReader fileReader = new FileReader(filename);
        bufferedReader = new BufferedReader(fileReader, bufferSize);
        // if (f.getAbsolutePath().endsWith(".gz")) {
        // readFileToLinesZipped(f);
        // } else {
        // readFileToLinesUnzipped(f);
        // }

    }

    public void readFileToLinesUnzipped(File f) throws IOException {
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        ArrayList<String> lines = new ArrayList<String>();
        String line;
        while ((line = br.readLine()) != null) {
            lines.add(line);
        }
        br.close();
        fr.close();
        this.lines = lines.toArray(new String[lines.size()]);
    }

    public HERV readNext() throws IOException {
        String nextLine = bufferedReader.readLine();
        HERV actualObject = null;
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

            // String strandOnChromosomeString = Aline[11];
            // byte strandOnChromosome = Byte.valueOf(strandOnChromosomeString);

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
        String value = chromosomString.substring(3);
        value = value.trim();
        value = value.toLowerCase();
        if (value.equals("x")) {
            return 23;
        }

        if (value.equals("y")) {
            return 24;
        }

        try {
            byte result = Byte.valueOf(value);
            return result;
        } catch (NumberFormatException ex) {
            log.error("Sequence number in the wrong format: {}; extracted number: {}", chromosomString, value);
            return -1;
        }
    }

    public long getOverallLines() {
        return overallLines;
    }

    public long getErrorLines() {
        return errorLines;
    }

    public static int TYPE = 1;
}
