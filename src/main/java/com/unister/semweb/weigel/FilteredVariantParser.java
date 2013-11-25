package com.unister.semweb.weigel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

public class FilteredVariantParser {
    String[] lines;
    int actLine;

    public FilteredVariantParser(String filename) throws IOException {
        File f = new File(filename);
        actLine = 0;
        if (f.getAbsolutePath().endsWith(".gz")) {
            readFileToLinesZipped(f);
        } else {
            readFileToLinesUnzipped(f);
        }

    }

    public void readFileToLinesZipped(File f) throws IOException {
        FileInputStream fis = new FileInputStream(f);
        GZIPInputStream gis = new GZIPInputStream(fis);
        BufferedReader in = new BufferedReader(new InputStreamReader(gis));
        String line;
        ArrayList<String> lines = new ArrayList<String>();
        while ((line = in.readLine()) != null) {
            lines.add(line);
        }
        this.lines = lines.toArray(new String[lines.size()]);
    }

    public void readFileToLinesUnzipped(File f) throws IOException {
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        ArrayList<String> lines = new ArrayList<String>();
        String line;
        if ((line = br.readLine()) != null) {
            lines.add(line);
        } else {
            br.close();
        }
        this.lines = lines.toArray(new String[lines.size()]);
    }

    public ArabReplace readNext() {
        if (actLine < lines.length) {
            return parseLine(lines[actLine++]);
        }
        return null;
    }

    public ArabReplace parseLine(String line) {
        String[] Aline = line.split("\t");
        String ecotype = Aline[0];
        Aline[1] = Aline[1].replace("chr", "");
        if (!Aline[1].equals("1") && !Aline[1].equals("2") && !Aline[1].equals("3") && !Aline[1].equals("4") && !Aline[1].equals("5")) {
            return null;
        }
        byte seqId = Byte.parseByte(Aline[1].replace("chr", ""));
        int pos = Integer.parseInt(Aline[2]);
        byte from = (byte) Aline[3].charAt(0);
        byte to = (byte) Aline[4].charAt(0);

        ArabReplace o = new ArabReplace();
        o.setBasePosition(pos);
        o.setEcotypeId((char) TYPE); //TODO:
        o.setSequenceId(seqId);
        o.setFrom(from);
        o.setTo(to);
        //        System.out.println(o);
        return o;
    }

    public static int TYPE = 1;
   
}
