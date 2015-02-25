package org.xbib.elasticsearch.jdbc.crypt;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.common.crypt.Codec;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

public class CryptTest {

    @Test
    public void crypt() throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeySpecException, IllegalBlockSizeException, BadPaddingException, InvalidAlgorithmParameterException, InvalidKeyException {
        Codec codec = new Codec();
        codec.createSecretKey("mammouth", 32);
        System.err.println(codec.getKeySpec().getKeyLength());
        String encrypted = codec.encrypt("Jörg is a software engineer.");
        System.err.println(encrypted);
        String decrypted = codec.decrypt(encrypted, codec.getSecretKey());
        Assert.assertEquals(decrypted, "Jörg is a software engineer.");
    }
}
