package com.unister.semweb.biodrums.weigel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Martin Nettling
 */
public class FilteredVariantParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilteredVariantParser.class);

    String[] lines;
    int curLine;
    int ecotype_id;

    /**
     * 
     * @param filename
     * @param ecotype_id
     * @throws IOException
     */
    public FilteredVariantParser(String filename, int ecotype_id) throws IOException {
        File f = new File(filename);
        curLine = 0;
        if (f.getAbsolutePath().endsWith(".gz")) {
            readFileToLinesZipped(f);
        } else {
            readFileToLinesUnzipped(f);
        }
        this.ecotype_id = ecotype_id;
        LOGGER.info("Read {} lines from {}", lines.length, filename);
    }

    private void readFileToLinesZipped(File f) throws IOException {
        FileInputStream fis = new FileInputStream(f);
        GZIPInputStream gis = new GZIPInputStream(fis);
        this.readStream(gis);
        gis.close();
        fis.close();
    }

    private void readFileToLinesUnzipped(File f) throws IOException {
        FileInputStream fis = new FileInputStream(f);
        this.readStream(fis);
        fis.close();
    }

    private void readStream(InputStream is) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        this.readLines(br);
        br.close();
    }

    private void readLines(BufferedReader reader) throws IOException {
        ArrayList<String> lines = new ArrayList<String>();
        String line;
        while ((line = reader.readLine()) != null) {
            lines.add(line);
        }
        reader.close();
        this.lines = lines.toArray(new String[lines.size()]);
    }

    /**
     * This method to read the next correct line from the underlying file. It parses this line and instantiates a new
     * {@link SNP} object.
     * 
     * @return the next {@link SNP}-object. NULL if no next object can be read.
     */
    public SNP readNext() {
        if (curLine < lines.length) {
            return parseLine(lines[curLine++]);
        }
        return null;
    }

    /**
     * This method parses the given line and generates a new {@link SNP}-object.
     * 
     * @param line
     *            the line to parse.
     * @return the {@link SNP}-object presented by the given line.
     */
    private SNP parseLine(String line) {
        String[] Aline = line.split("\t");
        String ecotype = Aline[0]; // TODO: read ecotype_id from a mapping
        Aline[1] = Aline[1].replace("chr", "");
        if (!Aline[1].equals("1") && !Aline[1].equals("2") && !Aline[1].equals("3") && !Aline[1].equals("4")
                && !Aline[1].equals("5")) {
            return null;
        }
        byte seqId = Byte.parseByte(Aline[1].replace("chr", ""));
        int pos = Integer.parseInt(Aline[2]);
        byte from = (byte) Aline[3].charAt(0);
        byte to = (byte) Aline[4].charAt(0);

        SNP o = new SNP();
        o.setBasePosition(pos);
        o.setEcotypeId((char) ecotype_id);
        o.setSequenceId(seqId);
        o.setFrom(from);
        o.setTo(to);
        return o;
    }
}
