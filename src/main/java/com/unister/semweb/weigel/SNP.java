package com.unister.semweb.weigel;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.unister.semweb.drums.file.HeaderIndexFile;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.utils.Bytes;
import com.unister.semweb.herv.HERV;

/**
 * This class represents a SNP. It can be stored in {@link HeaderIndexFile}s. An object of this class needs 9 bytes
 * in Byte-Representation. The first 7 byte of this representation represent the key. <br>
 * <br>
 * <code>
 * --------------------- 9 bytes --------------------<br>
 * .... key ................... | .... value ........<br>
 * seq id .| position | ecotype | from-base | to base<br>
 * 1 bytes | 4 bytes .| 2 bytes | 1 bytes ..| 1 bytes<br>
 * --------------------------------------------------<br>
 * </code><br>
 * <br>
 * Use the methods {@link #initFromByteBuffer(ByteBuffer)} and {@link #toByteBuffer()} to handle the byte-streams stored
 * in {@link HeaderIndexFile}.
 * 
 * @author Martin Nettling
 */
public class SNP extends AbstractKVStorable {
    private static final long serialVersionUID = 3226841314268658893L;

    /**
     * the size the object needs in byte, if we want to write it in a byte-array. If you make changes to the functions
     * <code>initFromByteBuffer(...)</code> and <code>toByteBuffer(...)</code>
     */
    /**
     * the number of bytes the object needs , if we want to write it in a byte-array. If you make changes to the
     * functions <code>initFromByteBuffer(...)</code> and <code>toByteBuffer(...)</code>
     */
    public static final int ELEMENT_SIZE = 9;

    /** The size of the key */
    public static final int KEY_SIZE = 7;

    /** offset of "sequence id" value in {@link SNP#key} */
    public static final int KEY_OFFEST_SEQUENCEID = 0;
    /** offset of "position" on sequence value in {@link SNP#key} */
    public static final int KEY_OFFEST_POSITION = 1;
    /** offset of "ecotype_id" value in {@link SNP#key} */
    public static final int KEY_OFFSET_ECOTYPE = 5;

    // ####### Data
    /** The unmutated base in the reference genome */
    private byte from = -1;
    /** The mutated base in the mapped genome */
    private byte to = -1;

    /**
     * Generates a new {@link SNP}-instance. Initializes empty {@link HERV#key}.
     */
    public SNP() {
        this.key = new byte[KEY_SIZE];
    }

    /**
     * sets the DNA-base from which was mutated
     * 
     * @param b
     *            the base to set
     */
    public void setFrom(byte b) {
        from = b;
    }

    /** @return the DNA-base from which was mutated */
    public byte getFrom() {
        return from;
    }

    /**
     * sets the DNA-base to which was mutated
     * 
     * @param b
     *            the base to set
     */
    public void setTo(byte b) {
        to = b;
    }

    /** @return the DNA-base to which was mutated */
    public byte getTo() {
        return to;
    }

    /**
     * Sets the id of the ecotype. The mapping from ecotype id to ecotype name must be stored elsewhere.
     * 
     * @param id
     */
    public void setEcotypeId(char id) {
        Bytes.putChar(key, KEY_OFFSET_ECOTYPE, id);
    }

    /**
     * Sets the sequence id. In case of A.thaliana the id is between 1 and 5.
     * 
     * @param id
     *            the sequence id
     */
    public void setSequenceId(byte id) {
        key[KEY_OFFEST_SEQUENCEID] = id;
    }

    /**
     * Sets the position of the SNP on a sequence.
     * 
     * @param position
     */
    public void setBasePosition(int position) {
        Bytes.putInt(key, KEY_OFFEST_POSITION, position);
    }

    /** @return the id of the ecotype */
    public char getEcotypeId() {
        return ByteBuffer.wrap(key).getChar(KEY_OFFSET_ECOTYPE);
    }

    /** @return the sequence id */
    public byte getSequenceId() {
        return ByteBuffer.wrap(key).get(KEY_OFFEST_SEQUENCEID);
    }

    /** @return the position of the SNP */
    public int getBasePosition() {
        return ByteBuffer.wrap(key).getInt(KEY_OFFEST_POSITION);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getSequenceId() + " ");
        sb.append(getBasePosition() + " ");
        sb.append((int) getEcotypeId() + ": ");
        sb.append((char) from);
        sb.append(" -> ");
        sb.append((char) to);
        return sb.toString();
    }

    @Override
    public void initFromByteBuffer(ByteBuffer bb) {
        bb.position(0);
        bb.get(key);
        from = bb.get();
        to = bb.get();
    }

    @SuppressWarnings("unchecked")
    @Override
    public SNP fromByteBuffer(ByteBuffer bb) {
        SNP r = new SNP();
        r.initFromByteBuffer(bb);
        return r;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        ByteBuffer bb = ByteBuffer.allocate(ELEMENT_SIZE);
        bb.put(key);
        bb.put(from);
        bb.put(to);
        return bb;
    }

    @Override
    public SNP clone() {
        ByteBuffer bytebuffer = this.toByteBuffer();
        return fromByteBuffer(bytebuffer);
    }

    @SuppressWarnings("unchecked")
    @Override
    public SNP merge(AbstractKVStorable element) {
        return (SNP) element;
    }

    @Override
    public void update(AbstractKVStorable element) {
        SNP castedElement = (SNP) element;
        this.from = castedElement.from;
        this.to = castedElement.to;
    }

    /**
     * @param toCompare
     * @return true, if the given {@link SNP} is equal to this {@link SNP}
     */
    public boolean equals(SNP toCompare) {
        if (!Arrays.equals(key, toCompare.key)) {
            return false;
        }

        if (to != toCompare.to) {
            return false;
        }

        if (from != toCompare.from) {
            return false;
        }

        return true;
    }
}
