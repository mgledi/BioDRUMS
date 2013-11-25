package com.unister.semweb.weigel;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.unister.semweb.drums.file.HeaderIndexFile;
import com.unister.semweb.drums.storable.AbstractKVStorable;

/**
 * This class represents a replace. It can be stored in {@link HeaderIndexFile}s. An object of this class needs 26 bytes
 * in Byte-Representation. The first 8 byte of this representation belongs to the key. <br>
 * <br>
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
public class ArabReplace extends AbstractKVStorable {

    /**  */
    private static final long serialVersionUID = 1L;

    /**
     * the size the object needs in byte, if we want to write it in a byte-array. If you make changes to the functions
     * <code>initFromByteBuffer(...)</code> and <code>toByteBuffer(...)</code>
     */
    public final int byteBufferSize = 9;

    public final int keySize = 7;

    public static final int sequenceIdOffset = 0;
    public static final int basePositionOffset = 1;
    public static final int ecotypeIdOffset = 5;

    // ####### Data
    private byte from;
    private byte to;

    public ArabReplace() {
        this.key = new byte[keySize];
    }

    /** sets the DNA-base from which was mutated */
    public void setFrom(byte b) {
        from = b;
    }

    /** returns the DNA-base from which was mutated */
    public byte getFrom() {
        return from;
    }

    /** sets the DNA-base to which was mutated */
    public void setTo(byte b) {
        to = b;
    }

    /** returns the DNA-base from which was mutated */
    public byte getTo() {
        return to;
    }

    public void setEcotypeId(char id) {
        ByteBuffer.wrap(key).putChar(ecotypeIdOffset, id);
    }

    public void setSequenceId(byte id) {
        ByteBuffer.wrap(key).put(sequenceIdOffset, id);
    }

    public void setBasePosition(int position) {
        ByteBuffer.wrap(key).putInt(basePositionOffset, position);
    }

    public char getEcotypeId() {
        return ByteBuffer.wrap(key).getChar(ecotypeIdOffset);
    }

    public byte getSequenceId() {
        return ByteBuffer.wrap(key).get(sequenceIdOffset);
    }

    public int getBasePosition() {
        return ByteBuffer.wrap(key).getInt(basePositionOffset);
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

    @Override
    public ArabReplace fromByteBuffer(ByteBuffer bb) {
        ArabReplace r = new ArabReplace();
        r.initFromByteBuffer(bb);
        return r;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        ByteBuffer bb = ByteBuffer.allocate(byteBufferSize);
        bb.put(key);
        bb.put(from);
        bb.put(to);
        return bb;
    }

    @Override
    public ArabReplace clone() {
        ByteBuffer bytebuffer = this.toByteBuffer();
        return fromByteBuffer(bytebuffer);
    }

    @Override
    public ArabReplace merge(AbstractKVStorable element) {
        return (ArabReplace) element;
    }

    @Override
    public void update(AbstractKVStorable element) {
    	ArabReplace castedElement = (ArabReplace) element;
        this.from = castedElement.from;
        this.to = castedElement.to;
    }

    public boolean equals(ArabReplace toCompare) {
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
