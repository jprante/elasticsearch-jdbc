package org.xbib.elasticsearch.common.crypt;

import org.xbib.elasticsearch.common.util.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidKeySpecException;

public class Codec {

    private Cipher cipher;

    private SecretKeyFactory secretKeyFactory;

    private PBEKeySpec keySpec;

    private int saltLength;

    private int initializationVectorSeedLength;

    private SecureRandom secureRandom = new SecureRandom();

    private Base64.Decoder decoder = Base64.getDecoder();

    private Base64.Encoder encoder = Base64.getEncoder();

    private SecretKey secretKey;

    public Codec() throws NoSuchAlgorithmException, NoSuchPaddingException {
        this(16, 16);
    }

    public Codec(int saltLength, int initializationVectorSeedLength)
            throws NoSuchAlgorithmException, NoSuchPaddingException {
        this.saltLength = saltLength;
        this.initializationVectorSeedLength = initializationVectorSeedLength;
        cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
    }

    public void createSecretKey(String password, int hashIterations) throws InvalidKeySpecException, NoSuchAlgorithmException {
        int maxKeyLen = Cipher.getMaxAllowedKeyLength("AES");
        keySpec = new PBEKeySpec(password.toCharArray(),
                secureRandom.generateSeed(saltLength), hashIterations, maxKeyLen);
        secretKey = new SecretKeySpec(secretKeyFactory.generateSecret(keySpec).getEncoded(), "AES");
    }

    public SecretKey getSecretKey() {
        return secretKey;
    }

    public PBEKeySpec getKeySpec() {
        return keySpec;
    }

    public String getEncodedSecretKey(SecretKey secretKey) {
        return encoder.encodeToString(secretKey.getEncoded());
    }

    public SecretKey getDecodedSecretKey(String secretKey) {
        return new SecretKeySpec(decoder.decode(secretKey), "AES");
    }

    public String encrypt(String rawText)
            throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, IllegalBlockSizeException,
            InvalidAlgorithmParameterException, BadPaddingException {
        if (secretKey == null) {
            throw new IllegalArgumentException("no secret key");
        }
        byte[] seed = secureRandom.generateSeed(initializationVectorSeedLength);
        AlgorithmParameterSpec algorithmParameterSpec = new IvParameterSpec(seed);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, algorithmParameterSpec);
        byte[] encryptedMessageBytes = cipher.doFinal(rawText.getBytes());
        byte[] bytesToEncode = new byte[seed.length + encryptedMessageBytes.length];
        System.arraycopy(seed, 0, bytesToEncode, 0, seed.length);
        System.arraycopy(encryptedMessageBytes, 0, bytesToEncode, seed.length, encryptedMessageBytes.length);
        return encoder.encodeToString(bytesToEncode);
    }

    public String decrypt(String encryptedText, SecretKey secretKey)
            throws InvalidKeyException, IllegalBlockSizeException, BadPaddingException, InvalidAlgorithmParameterException {
        byte[] bytesToDecode = decoder.decode(encryptedText);
        byte[] emptySeed = new byte[initializationVectorSeedLength];
        System.arraycopy(bytesToDecode, 0, emptySeed, 0, initializationVectorSeedLength);
        cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(emptySeed));
        int messageDecryptedBytesLength = bytesToDecode.length - initializationVectorSeedLength;
        byte[] messageDecryptedBytes = new byte[messageDecryptedBytesLength];
        System.arraycopy(bytesToDecode, initializationVectorSeedLength, messageDecryptedBytes, 0, messageDecryptedBytesLength);
        return new String(cipher.doFinal(messageDecryptedBytes));
    }
}
