package com.unister.semweb.weigel;

import java.nio.ByteBuffer;

import com.unister.semweb.drums.utils.KeyUtils;

public class CreateHashFunction {

    public static void main(String[] args) throws Exception {
        int[] chrLength = { 34964571, 22037565, 25499034, 20862711, 31270811 };
        int fullLength = 134634692;
        int lowerBucketBound = 128;
        
        int basesPerBucket = fullLength / lowerBucketBound + 1;
        byte[] upperBound= new byte[5];
        byte[] lowerBound = new byte[5];
        int bucketId = 0;
        StringBuilder sb = new StringBuilder();
        for(int i=0; i < 5; i++) {
            ByteBuffer.wrap(lowerBound).put((byte) (i+1)).putInt(0);
            ByteBuffer.wrap(upperBound).put((byte) (i+1)).putInt(chrLength[i]);
            int buckets = chrLength[i] / basesPerBucket;
            String[] bucketIds = new String[buckets];
            for (int j = 0; j < buckets; j++) {
                if(bucketId < 10) {
                    bucketIds[j] = "0" + bucketId;
                } else {
                    bucketIds[j] = "" + bucketId;
                }
                bucketId ++;
            }
            
            sb.append(KeyUtils.generateRangeHashFunction(lowerBound, upperBound, bucketIds, ".db", "replace"));
        }
        sb.insert(0, "b\tb\tb\tb\tb\tb\tb\tbucket\tsize\n");
        String out = sb.toString().replace("replace", "255\t255\treplace");
        System.out.println(out);
    }
}
