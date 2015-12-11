package com.fline.hadoop.util;

import java.security.Key;
import java.security.Security;
import javax.crypto.Cipher;

public class EncryptionDecryption {

	private static String strDefaultKey = "wfkey";
 
	private Cipher encryptCipher = null;

	private Cipher decryptCipher = null;

	/**
	 * ��byte����ת��Ϊ����16���Ƶ��ַ���
	 * 
	 * @param arrB
	 *            ��Ҫת����byte����
	 * @return 16���Ʊ��ֵ��ַ���
	 * @throws Exception
	 */
	public static String byteArr2HexStr(byte[] arrB) throws Exception {
		int bLen = arrB.length;
		StringBuffer strBuffer = new StringBuffer(bLen * 2);
		for (int i = 0; i != bLen; ++i) {
			int intTmp = arrB[i];
			while (intTmp < 0) {
				intTmp = intTmp + 256;
			}
			if (intTmp < 16) {
				strBuffer.append("0");
			}
			strBuffer.append(Integer.toString(intTmp, 16));
		}
		return strBuffer.toString();
	}

	/**
	 * ������16���Ƶ��ַ���ת��Ϊbyte����
	 * 
	 * @param hexStr
	 * @return
	 * @throws Exception
	 */
	public static byte[] hexStr2ByteArr(String hexStr) throws Exception {
		byte[] arrB = hexStr.getBytes();
		int bLen = arrB.length;
		byte[] arrOut = new byte[bLen / 2];
		for (int i = 0; i < bLen; i = i + 2) {
			String strTmp = new String(arrB, i, 2);
			arrOut[i / 2] = (byte) Integer.parseInt(strTmp, 16);
		}
		return arrOut;
	}

	/**
	 * ��Ĭ��������Ӧ����Ĭ�ܳ�
	 * 
	 * @throws Exception
	 */
	public EncryptionDecryption() throws Exception {
		this(strDefaultKey);
	}

	/**
	 * ָ���ܳ׹��췽��
	 * 
	 * @param strKey
	 *            ָ�����ܳ�
	 * @throws Exception
	 */
	@SuppressWarnings("restriction")
	public EncryptionDecryption(String strKey) throws Exception {
		Security.addProvider(new com.sun.crypto.provider.SunJCE());
		Key key = getKey(strKey.getBytes());

		encryptCipher = Cipher.getInstance("DES");
		encryptCipher.init(Cipher.ENCRYPT_MODE, key);

		decryptCipher = Cipher.getInstance("DES");
		decryptCipher.init(Cipher.DECRYPT_MODE, key);
	}

	/**
	 * �����ֽ�����
	 * 
	 * @param arrB
	 *            ����ܵ��ֽ�����
	 * @return ���ܺ���ֽ�����
	 * @throws Exception
	 */
	public byte[] encrypt(byte[] arrB) throws Exception {
		return encryptCipher.doFinal(arrB);
	}

	/**
	 * �����ַ���
	 * 
	 * @param strIn
	 *            ����ܵ��ַ���
	 * @return ���ܺ���ַ���
	 * @throws Exception
	 */
	public String encrypt(String strIn) throws Exception {
		return byteArr2HexStr(encrypt(strIn.getBytes()));
	}

	/**
	 * �ܽ��ֽ�����
	 * 
	 * @param arrB
	 *            ���ܽ���ֽ�����
	 * @return �ܽ����ֽ�����
	 * @throws Exception
	 */
	public byte[] decrypt(byte[] arrB) throws Exception {
		return decryptCipher.doFinal(arrB);
	}

	/**
	 * �ܽ��ַ���
	 * 
	 * @param strIn
	 *            ���ܽ���ַ���
	 * @return �ܽ����ַ���
	 * @throws Exception
	 */
	public String decrypt(String strIn) throws Exception {
		try {
			return new String(decrypt(hexStr2ByteArr(strIn)));
		} catch (Exception e) {
			return "";
		}
	}

	/**
	 * ��ָ���ַ��������ܳף��ܳ�������ֽ�����ȳ�Ϊ8λ��ȱ��8λʱ�����0����Խ8λʱ��ֻȡ����8λ
	 * 
	 * @param arrBTmp
	 *            �ɹ��ַ������ֽ�����
	 * @return ���ɵ��ܳ�
	 * @throws Exception
	 */
	private Key getKey(byte[] arrBTmp) throws Exception {
		byte[] arrB = new byte[8]; // ��ĬΪ0
		for (int i = 0; i < arrBTmp.length && i < arrB.length; ++i) {
			arrB[i] = arrBTmp[i];
		}
		Key key = new javax.crypto.spec.SecretKeySpec(arrB, "DES");
		return key;
	}

}