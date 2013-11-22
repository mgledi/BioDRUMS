package com.unister.semweb.herv;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.unister.semweb.sdrum.file.HeaderIndexFile;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;

/**
 * This class represents a HERV position. It can be stored in {@link HeaderIndexFile}s. <br>
 * <code>
 * ------------ 9 bytes ---------<br>
 * key ... | from-base | to base <br>
 * 7 bytes | 1 bytes ..| 1 bytes <br>
 * ------------------------------<br>
 * </code><br>
 * <br>
 * Use the methods <code>initFromByteBuffer(...)</code> and <code>toByteBuffer(...)</code> to handle the byte-streams
 * stored in {@link HeaderIndexFile}.
 * 
 * @author m.gleditzsch
 */
public class HERV extends AbstractKVStorable {

    /**  */
    private static final long serialVersionUID = 1L;

    /**
     * the size the object needs in byte, if we want to write it in a byte-array. If you make changes to the functions
     * <code>initFromByteBuffer(...)</code> and <code>toByteBuffer(...)</code>
     */
    public static final int byteBufferSize = 24;

    public static final int keySize = 15;

    /* key offsets */
    public static final int CHROMOSOME = 0;
    public static final int START_POS_CHROMOSOME = 1;
    public static final int END_POS_CHROMOSOME = 5;
    public static final int START_POS_HERV = 9;
    public static final int END_POS_HERV = 11;
    public static final int HERV_ID= 13;

    /* value offsets */
    public static final int STRAND_ON_CHROMOSOME = 0;
    public static final int EVALUE = 1;

    // ####### Data
    // private byte from;
    // private byte to;

    public HERV() {
        key = new byte[keySize];
        value = new byte[byteBufferSize-keySize];
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    /**
     * Sets the key of this HERV data. The key consists of a chromosome, a start and end position at the chromosome, a
     * start end end position at HERV, an id of HERV, the strand on chromosome and an e-value.
     * 
     * @param chromosome
     * @param startPositionChromosome
     * @param endPositionChromosome
     * @param startHERV
     * @param endHERV
     * @param idHERV
     * @param strandAtChromosome
     * @param eValue
     */
    public void setKey(byte chromosome, int startPositionChromosome, int endPositionChromosome, char startHERV,
            char endHERV, char idHERV) {
        ByteBuffer converter = ByteBuffer.allocate(keySize);
        converter.put(chromosome);
        converter.putInt(startPositionChromosome);
        converter.putInt(endPositionChromosome);
        converter.putChar(startHERV);
        converter.putChar(endHERV);
        converter.putChar(idHERV);

        key = converter.array();
    }

    public byte getChromosome() {
        return key[0];
    }

    public int getStartPositionChromosome() {
        return ByteBuffer.wrap(key).getInt(START_POS_CHROMOSOME);
    }

    public int getEndPositionChromosome() {
        return ByteBuffer.wrap(key).getInt(END_POS_CHROMOSOME);
    }

    public char getStartHERV() {
        return ByteBuffer.wrap(key).getChar(START_POS_HERV);
    }

    public char getEndHERV() {
        return ByteBuffer.wrap(key).getChar(END_POS_HERV);
    }

    public char getIdHERV() {
        return ByteBuffer.wrap(key).getChar(HERV_ID);
    }

    public byte getStrandOnChromosome() {
        return ByteBuffer.wrap(value).get(STRAND_ON_CHROMOSOME);
    }

    public void setStrandOnChromosome(byte strandOnChromosome) {
        ByteBuffer.wrap(value).put(STRAND_ON_CHROMOSOME, strandOnChromosome);
    }

    public double getEValue() {
        return ByteBuffer.wrap(value).getDouble(EVALUE);
    }

    public void setEValue(double eValue) {
        ByteBuffer.wrap(value).putDouble(EVALUE, eValue);
    }

    public int getKeySize() {
        return keySize;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append((int)getChromosome()).append(" ");
        sb.append((int)getStartPositionChromosome()).append(" ");
        sb.append((int)getEndPositionChromosome()).append(" ");
        sb.append((int)getStartHERV()).append(" ");
        sb.append((int)getEndHERV()).append(" ");
        sb.append((int)getIdHERV()).append(" ");
        sb.append((int)getStrandOnChromosome()).append(" ");
        sb.append(getEValue()).append(" ");
        return sb.toString();
    }

    @Override
    public void initFromByteBuffer(ByteBuffer bb) {
        byte[] extractedKey = new byte[keySize];
        byte[] extractedVal = new byte[byteBufferSize - keySize];
        bb.get(extractedKey);
        bb.get(extractedVal);

        this.key = extractedKey;
        this.value = extractedVal;
    }

    @Override
    public HERV fromByteBuffer(ByteBuffer bb) {
        HERV r = new HERV();
        r.initFromByteBuffer(bb);
        return r;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        ByteBuffer bb = ByteBuffer.allocate(byteBufferSize).put(key).put(value);
        bb.rewind();
        return bb;
    }

    @Override
    public HERV clone() {
        ByteBuffer bytebuffer = this.toByteBuffer();
        return fromByteBuffer(bytebuffer);
    }

    @Override
    public int getByteBufferSize() {
        return byteBufferSize;
    }

//    @Override
//    public HERV merge(AbstractKVStorable element) {
//        return (HERV) element;
//    }

//    @Override
//    public void update(HERV element) {
//        this.setStrandOnChromosome(element.getStrandOnChromosome());
//        this.setEValue(element.getEValue());
//    }

    public boolean equals(HERV toCompare) {
        if (!Arrays.equals(key, toCompare.key)) {
            return false;
        }

        if (getStrandOnChromosome() != toCompare.getStrandOnChromosome()) {
            return false;
        }

        if (getEValue() != toCompare.getEValue()) {
            return false;
        }
        return true;
    }

	@Override
	public AbstractKVStorable merge(AbstractKVStorable element) {
		return (HERV) element;
		// TODO Auto-generated
	}

	@Override
	public void update(AbstractKVStorable element) {
		HERV castedElement = (HERV) element;
		this.setStrandOnChromosome(castedElement.getStrandOnChromosome());
        this.setEValue(castedElement.getEValue());
		
	}
}
