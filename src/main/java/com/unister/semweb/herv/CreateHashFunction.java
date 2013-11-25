package com.unister.semweb.herv;

import java.nio.ByteBuffer;

import com.unister.semweb.sdrum.utils.KeyUtils;

public class CreateHashFunction {

    public static void main(String[] args) throws Exception {
        int[] chrLength = {
                245203898,
                243315028,
                199411731,
                191610523,
                180967295,
                170740541,
                158431299,
                145908738,
                134505819,
                135480874,
                134978784,
                133464434,
                114151656,
                105311216,
                100114055,
                89995999,
                81691216,
                77753510,
                63790860,
                63644868,
                46976537,
                49476972,
                152634166,
                50961097
        };
        long fullLength = sum(chrLength);
        int lowerBucketBound = 256;

        long basesPerBucket = fullLength / lowerBucketBound + 1;
        byte[] upperBound = new byte[5];
        byte[] lowerBound = new byte[5];
        int bucketId = 0;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < chrLength.length; i++) {
            ByteBuffer.wrap(lowerBound).put((byte) (i + 1)).putInt(0);
            ByteBuffer.wrap(upperBound).put((byte) (i + 1)).putInt(chrLength[i]);
            int buckets = (int) (chrLength[i] / basesPerBucket);
            String[] bucketIds = new String[buckets];
            for (int j = 0; j < buckets; j++) {
                if (bucketId < 10) {
                    bucketIds[j] = "0" + bucketId;
                } else {
                    bucketIds[j] = "" + bucketId;
                }
                bucketId++;
            }

            sb.append(KeyUtils.generateHashFunctionString(lowerBound, upperBound, bucketIds, ".db", "replace"));
        }
        sb.insert(0, "b\tb\tb\tb\tb\tb\tb\tbucket\n");
        String out = sb.toString().replace("replace", "255\t255\treplace");
        System.out.println(out);
    }

    private static long sum(int[] toSumUp) {
        long finalSum = 0;
        for (int oneOperand : toSumUp) {
            finalSum = finalSum + oneOperand;
        }

        return finalSum;
    }
}
