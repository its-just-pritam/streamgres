package com.streamlite.broker.partition;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Optional;

public class PartitionHashGenerator {

    public static int getPartition(String messageKey, Integer partitions) {

        byte[] keyBytes;

        try {
            if (messageKey != null) {
                // Use SHA-256 for strong hashing
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                keyBytes = digest.digest(messageKey.getBytes(StandardCharsets.UTF_8));
            } else {
                SecureRandom random = SecureRandom.getInstanceStrong();
                keyBytes = new byte[32]; // 256-bit
                random.nextBytes(keyBytes);
            }

            // Take the first 4 bytes to map to an int
            int hash = ((keyBytes[0] & 0xFF) << 24) |
                    ((keyBytes[1] & 0xFF) << 16) |
                    ((keyBytes[2] & 0xFF) << 8) |
                    (keyBytes[3] & 0xFF);

            hash = Math.abs(hash);
            return hash % partitions;

        } catch (NoSuchAlgorithmException e) {
            int fallbackHash = Optional.ofNullable(messageKey)
                    .map(String::hashCode)
                    .orElse(new SecureRandom().nextInt());
            return (fallbackHash & Integer.MAX_VALUE) % partitions;
        }
    }

}
