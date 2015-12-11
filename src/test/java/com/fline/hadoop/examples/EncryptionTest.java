package com.fline.hadoop.examples;

import org.junit.Test;

import com.fline.hadoop.util.EncryptionDecryption;

public class EncryptionTest {
	@Test
	public void test() throws Exception {
		EncryptionDecryption des = new EncryptionDecryption("wf");
		byte[] rowkey = "322323".getBytes();
		String newStr = "";
		newStr = des.byteArr2HexStr(rowkey);
		System.out.println("加密后:   " + newStr);

		byte[] oldStr = des.hexStr2ByteArr(newStr);
		System.out.println("密解后：  " + oldStr);
	}
}