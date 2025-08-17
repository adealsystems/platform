/*
 * Copyright 2020-2025 ADEAL Systems GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.adealsystems.platform.orchestrator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;

public final class ChecksumGenerator {
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    private static final String HASH_ALG = "MD5";

    private ChecksumGenerator() {
    }

    public static String getChecksum(Serializable object) {
        byte[] serializedObject;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            serializedObject = baos.toByteArray();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Error serializing object!", ex);
        }

        try {
            MessageDigest md = MessageDigest.getInstance(HASH_ALG);
            md.update(serializedObject);
            byte[] hash = md.digest();
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("Unable to initialize MessageDigest!", ex);
        }
    }

    public static String getChecksum(Collection<? extends Serializable> collection) {
        byte[] serializedObject;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            if (collection != null) {
                for (Serializable item : collection) {
                    oos.writeObject(item);
                }
            }
            serializedObject = baos.toByteArray();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Error serializing object!", ex);
        }

        try {
            MessageDigest md = MessageDigest.getInstance(HASH_ALG);
            md.update(serializedObject);
            byte[] hash = md.digest();

            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("Unable to initialize MessageDigest!", ex);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }
}
