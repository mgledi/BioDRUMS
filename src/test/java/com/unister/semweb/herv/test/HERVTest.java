package com.unister.semweb.herv.test;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import com.unister.semweb.biodrums.herv.HERV;

/**
 * Tests the method of the {@link HERV}.
 * 
 * @author n.thieme
 * 
 */
public class HERVTest {
    /**
     * Tests the getter and setter methods of the {@link HERV}.
     */
    @Test
    public void getterSetterTest() {
        byte chromosome = (byte) 12;
        int startChromosome = 1374823;
        int endChromosome = 7987897;
        char startHERV = 1283;
        char endHERV = 28382;
        char idHERV = 26372;

        byte strandOnChromosome = (byte) 123;
        double eValue = 1346.84838238;

        HERV testObject = new HERV();
        testObject.setKey(chromosome, startChromosome, endChromosome, startHERV, endHERV, idHERV);
        testObject.setStrandOnChromosome(strandOnChromosome);
        testObject.setEValue(eValue);

        Assert.assertEquals(chromosome, testObject.getChromosome());
        Assert.assertEquals(startChromosome, testObject.getStartPositionChromosome());
        Assert.assertEquals(endChromosome, testObject.getEndPositionChromosome());
        Assert.assertEquals(startHERV, testObject.getStartHERV());
        Assert.assertEquals(endHERV, testObject.getEndHERV());
        Assert.assertEquals(idHERV, testObject.getIdHERV());
        Assert.assertEquals(strandOnChromosome, testObject.getStrandOnChromosome());
        Assert.assertEquals(eValue, testObject.getEValue(), 0.0);
    }

    /**
     * Tests the <code>initFromByteBuffer</code> method.
     */
    @Test
    public void initTest() {
        byte chromosome = (byte) 12;
        int startChromosome = 1374823;
        int endChromosome = 7987897;
        char startHERV = 1283;
        char endHERV = 28382;
        char idHERV = 26372;

        byte strandOnChromosome = (byte) 123;
        double eValue = 1346.84838238;

        HERV prototype = new HERV();
        ByteBuffer byteBufferObject = ByteBuffer.allocate(prototype.ELEMENT_SIZE);
        byteBufferObject.put(chromosome);
        byteBufferObject.putInt(startChromosome);
        byteBufferObject.putInt(endChromosome);
        byteBufferObject.putChar(startHERV);
        byteBufferObject.putChar(endHERV);
        byteBufferObject.putChar(idHERV);
        byteBufferObject.put(strandOnChromosome);
        byteBufferObject.putDouble(eValue);
        byteBufferObject.flip();

        HERV testObject = new HERV();
        testObject.initFromByteBuffer(byteBufferObject);

        Assert.assertEquals(chromosome, testObject.getChromosome());
        Assert.assertEquals(startChromosome, testObject.getStartPositionChromosome());
        Assert.assertEquals(endChromosome, testObject.getEndPositionChromosome());
        Assert.assertEquals(startHERV, testObject.getStartHERV());
        Assert.assertEquals(endHERV, testObject.getEndHERV());
        Assert.assertEquals(idHERV, testObject.getIdHERV());
        Assert.assertEquals(strandOnChromosome, testObject.getStrandOnChromosome());
        Assert.assertEquals(eValue, testObject.getEValue(), 0.0);
    }

    /**
     * Tests the <code>fromByteBuffer</code> method
     */
    @Test
    public void fromTest() {
        byte chromosome = (byte) 12;
        int startChromosome = 1374823;
        int endChromosome = 7987897;
        char startHERV = 1283;
        char endHERV = 28382;
        char idHERV = 26372;

        byte strandOnChromosome = (byte) 123;
        double eValue = 1346.84838238;

        HERV prototype = new HERV();
        ByteBuffer byteBufferObject = ByteBuffer.allocate(prototype.ELEMENT_SIZE);
        byteBufferObject.put(chromosome);
        byteBufferObject.putInt(startChromosome);
        byteBufferObject.putInt(endChromosome);
        byteBufferObject.putChar(startHERV);
        byteBufferObject.putChar(endHERV);
        byteBufferObject.putChar(idHERV);
        byteBufferObject.put(strandOnChromosome);
        byteBufferObject.putDouble(eValue);
        byteBufferObject.flip();

        HERV testObject = prototype.fromByteBuffer(byteBufferObject);

        Assert.assertEquals(chromosome, testObject.getChromosome());
        Assert.assertEquals(startChromosome, testObject.getStartPositionChromosome());
        Assert.assertEquals(endChromosome, testObject.getEndPositionChromosome());
        Assert.assertEquals(startHERV, testObject.getStartHERV());
        Assert.assertEquals(endHERV, testObject.getEndHERV());
        Assert.assertEquals(idHERV, testObject.getIdHERV());
        Assert.assertEquals(strandOnChromosome, testObject.getStrandOnChromosome());
        Assert.assertEquals(eValue, testObject.getEValue(), 0.0);
    }

    /**
     * Tests the <code>toByteBuffer</code> method.
     */
    @Test
    public void toByteBuffer() {
        byte chromosome = (byte) 12;
        int startChromosome = 1374823;
        int endChromosome = 7987897;
        char startHERV = 1283;
        char endHERV = 28382;
        char idHERV = 26372;

        byte strandOnChromosome = (byte) 123;
        double eValue = 1346.84838238;

        HERV prototype = new HERV();
        ByteBuffer byteBufferObject = ByteBuffer.allocate(prototype.ELEMENT_SIZE);
        byteBufferObject.put(chromosome);
        byteBufferObject.putInt(startChromosome);
        byteBufferObject.putInt(endChromosome);
        byteBufferObject.putChar(startHERV);
        byteBufferObject.putChar(endHERV);
        byteBufferObject.putChar(idHERV);
        byteBufferObject.put(strandOnChromosome);
        byteBufferObject.putDouble(eValue);
        byteBufferObject.flip();

        HERV testObject = new HERV();
        testObject.setKey(chromosome, startChromosome, endChromosome, startHERV, endHERV, idHERV);
        testObject.setStrandOnChromosome(strandOnChromosome);
        testObject.setEValue(eValue);

        ByteBuffer testObjectBuffer = testObject.toByteBuffer();

        Assert.assertTrue(byteBufferObject.equals(testObjectBuffer));
    }

}
