package com.unister.semweb.herv;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import com.unister.semweb.drums.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.drums.file.HeaderIndexFile;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.utils.Bytes;
import com.unister.semweb.drums.utils.KeyUtils;

/**
 * This class represents a position of a HERV in DNA. It can be stored in {@link HeaderIndexFile}s. <br>
 * <br>
 * Use the methods {@link #initFromByteBuffer(ByteBuffer)} and {@link #toByteBuffer()} to handle the byte-streams stored
 * in {@link HeaderIndexFile}.
 * 
 * @author Martin Nettling
 */
public class HERV extends AbstractKVStorable {
    private static final long serialVersionUID = -5631191270265012150L;

    /** The lengths of the human chromosomes in basepairs */
    public static final int[] HUMAN_CHROMOSOME_LENGTHS = {
            245203898, // chromosome 1
            243315028, // chromosome 2
            199411731, // chromosome 3
            191610523, // chromosome 4
            180967295, // chromosome 5
            170740541, // chromosome 6
            158431299, // chromosome 7
            145908738, // chromosome 8
            134505819, // chromosome 9
            135480874, // chromosome 10
            134978784, // chromosome 11
            133464434, // chromosome 12
            114151656, // chromosome 13
            105311216, // chromosome 14
            100114055, // chromosome 15
            89995999, // chromosome 16
            81691216, // chromosome 17
            77753510, // chromosome 18
            63790860, // chromosome 19
            63644868, // chromosome 20
            46976537, // chromosome 21
            49476972, // chromosome 22
            152634166, // chromosome X
            50961097 // chromosome Y
    };

    /**
     * the number of bytes the object needs , if we want to write it in a byte-array. If you make changes to the
     * functions <code>initFromByteBuffer(...)</code> and <code>toByteBuffer(...)</code>
     */
    public static final int ELEMENT_SIZE = 24;

    /** The size of the key */
    public static final int KEY_SIZE = 15;

    /* key offsets */
    /** offset of "chromosome" value in {@link HERV#key} */
    public static final int KEY_OFFSET_CHROMOSOME = 0;
    /** offset of "start position on chromosome" value in {@link HERV#key} */
    public static final int KEY_OFFSET_START_POS_CHROMOSOME = 1;
    /** offset of "end position on chromosome" value in {@link HERV#key} */
    public static final int KEY_OFFSET_END_POS_CHROMOSOME = 5;
    /** offset of "start position in HERV" value in {@link HERV#key} */
    public static final int KEY_OFFSET_START_POS_HERV = 9;
    /** offset of "end position in HERV" value in {@link HERV#key} */
    public static final int KEY_OFFSET_END_POS_HERV = 11;
    /** offset of "HERV-id" value in {@link HERV#key} */
    public static final int KEY_OFFSET_HERV_ID = 13;

    /* value offsets */
    /** offset of "STRAND" value in {@link HERV#value} */
    public static final int VALUE_OFFSET_STRAND_ON_CHROMOSOME = 0;
    /** offset of "E-VALUE" value in {@link HERV#value} */
    public static final int VALUE_OFFSET_EVALUE = 1;

    /**
     * Generates a new {@link HERV}-instance. Initializes empty {@link HERV#key} and {@link HERV#value}.
     */
    public HERV() {
        key = new byte[KEY_SIZE];
        value = new byte[ELEMENT_SIZE - KEY_SIZE];
    }

    /**
     * Sets the key of this HERV data. The key consists of a chromosome, a start and end position at the chromosome, a
     * start end end position at HERV, an id of HERV, the strand on chromosome and an e-value.
     * 
     * @param chromosome
     *            the chromosome number, the smallest chromosome has number 1
     * @param startPositionChromosome
     *            the start position of the mapping in the reference DNA
     * @param endPositionChromosome
     *            the end position of the mapping in the reference DNA
     * @param startHERV
     *            the start position of the mapping in the HERV
     * @param endHERV
     *            the end position of the mapping in the HERV
     * @param idHERV
     *            the id of the HERV
     */
    public void setKey(
            byte chromosome,
            int startPositionChromosome,
            int endPositionChromosome,
            char startHERV,
            char endHERV,
            char idHERV) {
        ByteBuffer converter = ByteBuffer.allocate(KEY_SIZE);
        converter.put(chromosome);
        converter.putInt(startPositionChromosome);
        converter.putInt(endPositionChromosome);
        converter.putChar(startHERV);
        converter.putChar(endHERV);
        converter.putChar(idHERV);

        key = converter.array();
    }

    /** @return the chromosome-id */
    public byte getChromosome() {
        return key[KEY_OFFSET_CHROMOSOME];
    }

    /** @return the start position on the chromosome */
    public int getStartPositionChromosome() {
        return Bytes.toInt(key, KEY_OFFSET_START_POS_CHROMOSOME);
    }

    /** @return the end position on the chromosome */
    public int getEndPositionChromosome() {
        return Bytes.toInt(key, KEY_OFFSET_END_POS_CHROMOSOME);
    }

    /** @return the start position of mapping in the HERV */
    public char getStartHERV() {
        return Bytes.toChar(key, KEY_OFFSET_START_POS_HERV);
    }

    /** @return the end position of mapping in the HERV */
    public char getEndHERV() {
        return Bytes.toChar(key, KEY_OFFSET_END_POS_HERV);
    }

    /** @return the id of the HERV */
    public char getIdHERV() {
        return Bytes.toChar(key, KEY_OFFSET_HERV_ID);
    }

    /** @return the strand of the chromosome, where the HERV can be found */
    public byte getStrandOnChromosome() {
        return value[VALUE_OFFSET_STRAND_ON_CHROMOSOME];
    }

    /**
     * @param strandOnChromosome
     */
    public void setStrandOnChromosome(byte strandOnChromosome) {
        value[VALUE_OFFSET_STRAND_ON_CHROMOSOME] = strandOnChromosome;
    }

    /** @return the e-value of this HERV */
    public double getEValue() {
        return Bytes.toDouble(value, VALUE_OFFSET_EVALUE);
    }

    /**
     * Sets the e-value for this HERV
     * 
     * @param eValue
     */
    public void setEValue(double eValue) {
        Bytes.putDouble(value, VALUE_OFFSET_EVALUE, eValue);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append((int) getChromosome()).append(" ");
        sb.append((int) getStartPositionChromosome()).append(" ");
        sb.append((int) getEndPositionChromosome()).append(" ");
        sb.append((int) getStartHERV()).append(" ");
        sb.append((int) getEndHERV()).append(" ");
        sb.append((int) getIdHERV()).append(" ");
        sb.append((int) getStrandOnChromosome()).append(" ");
        sb.append(getEValue()).append(" ");
        return sb.toString();
    }

    @Override
    public void initFromByteBuffer(ByteBuffer bb) {
        byte[] extractedKey = new byte[KEY_SIZE];
        byte[] extractedVal = new byte[ELEMENT_SIZE - KEY_SIZE];
        bb.get(extractedKey);
        bb.get(extractedVal);

        this.key = extractedKey;
        this.value = extractedVal;
    }

    @SuppressWarnings("unchecked")
    @Override
    public HERV fromByteBuffer(ByteBuffer bb) {
        HERV r = new HERV();
        r.initFromByteBuffer(bb);
        return r;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        ByteBuffer bb = ByteBuffer.allocate(ELEMENT_SIZE).put(key).put(value);
        bb.rewind();
        return bb;
    }

    @Override
    public HERV clone() {
        ByteBuffer bytebuffer = this.toByteBuffer();
        return fromByteBuffer(bytebuffer);
    }

    @SuppressWarnings("javadoc")
    public boolean equals(HERV toCompare) {
        if (!Arrays.equals(key, toCompare.key)) {
            return false;
        }

        if (!Arrays.equals(value, toCompare.value)) {
            return false;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public AbstractKVStorable merge(AbstractKVStorable element) {
        return element;
    }

    @Override
    public void update(AbstractKVStorable element) {
        HERV castedElement = (HERV) element;
        this.setStrandOnChromosome(castedElement.getStrandOnChromosome());
        this.setEValue(castedElement.getEValue());

    }

    /**
     * This method determines a good {@link RangeHashFunction} for {@link #HUMAN_CHROMOSOME_LENGTHS}.
     * 
     * @return a {@link RangeHashFunction} for {@link HERV}-data
     */
    public static RangeHashFunction createHashFunction() {

        long fullLength = sum(HUMAN_CHROMOSOME_LENGTHS);
        int lowerBucketBound = 256;

        long basesPerBucket = fullLength / lowerBucketBound + 1;
        byte[] upperBound = new byte[7];
        byte[] lowerBound = new byte[7];

        int bucketId = 0;
        ArrayList<byte[]> maxKeyValues = new ArrayList<byte[]>();
        ArrayList<String> bucketNames = new ArrayList<String>();
        // generate for each chromosome some buckets, depending on its length
        for (int i = 0; i < HUMAN_CHROMOSOME_LENGTHS.length; i++) {
            ByteBuffer.wrap(lowerBound).put((byte) (i + 1)).putInt(0); // put chromosome number
            ByteBuffer.wrap(upperBound).put((byte) (i + 1)).putInt(HUMAN_CHROMOSOME_LENGTHS[i]); // put length of
                                                                                                 // chromosome
            int buckets = (int) (HUMAN_CHROMOSOME_LENGTHS[i] / basesPerBucket);
            byte[][] rangesTmp = KeyUtils.getMaxValsPerRange(lowerBound, upperBound, buckets);

            for (int j = 0; j < buckets; j++) {
                if (bucketId < 10) {
                    bucketNames.add("0" + bucketId);
                } else {
                    bucketNames.add("" + bucketId);
                }
                rangesTmp[j][5] = rangesTmp[j][6] = (byte) 255;
                maxKeyValues.add(rangesTmp[j]);
                bucketId++;
            }

        }
        RangeHashFunction hashfunction = new RangeHashFunction(
                maxKeyValues.toArray(new byte[0][]),
                bucketNames.toArray(new String[0]),
                "HERV_RangeHashFunction.txt");
        return hashfunction;
    }

    private static long sum(int[] summands) {
        long finalSum = 0;
        for (long summand : summands) {
            finalSum += summand;
        }
        return finalSum;
    }
}
