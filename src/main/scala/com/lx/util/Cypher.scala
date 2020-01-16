package com.lx.util

import java.util.Random

object Cypher {
  /**
    * RSA解密
    */
  def decrypt_rsa(rsaKeydateBytes: Array[Byte], privateKeyBytes: Array[Byte]) = {
    import java.security.KeyFactory
    import java.security.spec.PKCS8EncodedKeySpec
    import javax.crypto.Cipher
    // rsa 128位块
    // 创建一个PKCS8EncodedKeySpec对象
    val privSpec = new PKCS8EncodedKeySpec(privateKeyBytes)
    // 创建一个RSA密匙工厂实例
    val keyFactory = KeyFactory.getInstance("RSA")
    // Cipher对象实际完成解密操作
    val cipher = Cipher.getInstance("RSA")
    // 用公匙初始化Cipher对象
    cipher.init(Cipher.DECRYPT_MODE, keyFactory.generatePrivate(privSpec))
    // 真正开始加密操作
    cipher.doFinal(rsaKeydateBytes)
  }


  /**
    * DES解密
    */
  def decrypt_des(desDateBytes: Array[Byte], desKeyBytes: Array[Byte]) = {
    import javax.crypto.Cipher
    import javax.crypto.spec.SecretKeySpec
    // 创建一个SecretKeySpec对象
    val secretKey = new SecretKeySpec(desKeyBytes, "DES")
    // Cipher对象实际完成解密操作
    val encipher = Cipher.getInstance("DES")
    // 用密匙初始化Cipher对象
    encipher.init(Cipher.DECRYPT_MODE, secretKey)
    // 真正开始解密操作
    encipher.doFinal(desDateBytes)
  }

  /**
    * RSA加密
    */
  def encrypt_rsa(dateBytes: Array[Byte], publicKeyBytes: Array[Byte]) = {
    import java.security.KeyFactory
    import java.security.spec.X509EncodedKeySpec
    import javax.crypto.Cipher
    // 创建一个X509EncodedKeySpec对象
    val keyPec = new X509EncodedKeySpec(publicKeyBytes)
    // 创建一个RSA密匙工厂实例
    val keyFactory = KeyFactory.getInstance("RSA")
    // 创建一个509公钥
    val publicKey = keyFactory.generatePublic(keyPec)
    // Cipher对象实际完成解密操作
    val cipher = Cipher.getInstance("RSA")
    // 用公匙初始化Cipher对象
    cipher.init(Cipher.ENCRYPT_MODE, publicKey)
    // 真正开始加密操作
    cipher.doFinal(dateBytes)
  }

  /**
    * DES加密
    */
  def encrypt_des(desDateBytes: Array[Byte], desKeyBytes: Array[Byte]) = {
    import java.security.SecureRandom
    import javax.crypto.{Cipher, SecretKeyFactory}
    import javax.crypto.spec.DESKeySpec
    // DES算法要求有一个可信任的随机数源
    val secureRandom = new SecureRandom
    // 创建一个DESKeySpec对象
    val desKeySpec = new DESKeySpec(desKeyBytes)
    //创建一个密匙工厂，然后用它把DESKeySpec转换成
    val keyFactory = SecretKeyFactory.getInstance("DES")
    val secretKey = keyFactory.generateSecret(desKeySpec)
    //Cipher对象实际完成加密操作
    val cipher = Cipher.getInstance("DES")
    //用密匙初始化Cipher对象
    cipher.init(Cipher.ENCRYPT_MODE, secretKey, secureRandom)
    //获取数据并加密，正式执行加密操作
    cipher.doFinal(desDateBytes)
  }

  /**
    * 二进制私钥读入
    */
  def getkeyFile(filePath: String) = {
    import java.io.File
    import java.io.FileInputStream
    val input = new FileInputStream(new File(filePath))
    val keyBytes = new Array[Byte](input.available())
    input.read(keyBytes)
    input.close
    keyBytes
  }

  /**
    * 成一个n位的随机数
    *
    * @param size
    * @return
    */
  def randomNum(size: Int): String = {
    var i: Int = 0
    var count = 0
    val str = Array('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '=', '+')
    val pwd = new StringBuffer("")
    val r = new Random();
    while (count < size) {
      i = Math.abs(r.nextInt(str.size))
      if (i >= 0 && i < str.length) {
        pwd.append(str(i))
        count += 1
      }
    }
    pwd.toString
  }

}
