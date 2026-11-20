package compression.common;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class Hashing {

    // SplitMix64 mixing function
    static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    // Same (seed, column, key) -> same value: the data does not depend on the partitions
    public static long hash(long seed, String column, long key) {
        long h = mix(seed ^ 0x9e3779b97f4a7c15L);
        h = mix(h ^ column.hashCode());
        h = mix(h ^ key);
        return h >>> 1;
    }

    // number between low and high (included)
    public static long uniform(long seed, String column, long key, long low, long high) {
        return low + Math.floorMod(hash(seed, column, key), high - low + 1);
    }

    // sha256(sha256(...(value))), `rounds` times, as a hex string
    public static String iteratedSha256(String value, int rounds) throws Exception {
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        String current = value;
        for (int i = 0; i < rounds; i++) {
            byte[] digest = sha256.digest(current.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) {
                hex.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
            }
            current = hex.toString();
        }
        return current;
    }

    // first 15 hex digits (60 bits) of a digest as a long, so that we can take a max()
    public static long prefix(String hexDigest) {
        return Long.parseLong(hexDigest.substring(0, 15), 16);
    }
}
