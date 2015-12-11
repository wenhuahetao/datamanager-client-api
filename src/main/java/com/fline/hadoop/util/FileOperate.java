package com.fline.hadoop.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * <b>�ļ���ȡ��</b><br />
 * 1�����ֽڶ�ȡ�ļ�����<br />
 * 2�����ַ���ȡ�ļ�����<br />
 * 3�����ж�ȡ�ļ�����<br />
 * 
 * @author qin_xijuan
 * 
 */
public class FileOperate {

	private static final String FILE_PATH = "d:/work/jipinwodi.txt";

	/**
	 * ���ֽ�Ϊ��λ��д�ļ�����
	 * 
	 * @param filePath
	 *            ����Ҫ��ȡ���ļ�·��
	 */
	public static void readFileByByte(String filePath) {
		File file = new File(filePath);
		// InputStream:�˳������Ǳ�ʾ�ֽ���������������ĳ��ࡣ
		InputStream ins = null;
		OutputStream outs = null;
		try {
			// FileInputStream:���ļ�ϵͳ�е�ĳ���ļ��л�������ֽڡ�
			ins = new FileInputStream(file);
			outs = new FileOutputStream("d:/work/readFileByByte.txt");
			int temp;
			// read():���������ж�ȡ���ݵ���һ���ֽڡ�
			while ((temp = ins.read()) != -1) {
				outs.write(temp);
			}
		} catch (Exception e) {
			e.getStackTrace();
		} finally {
			if (ins != null && outs != null) {
				try {
					outs.close();
					ins.close();
				} catch (IOException e) {
					e.getStackTrace();
				}
			}
		}
	}

	/**
	 * �����ļ���
	 */
	public static void mkdir(String path) {
		File fd = null;
		try {
			fd = new File(path);
			if (!fd.exists()) {
				fd.mkdirs();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			fd = null;
		}
	}

	/**
	 * д�ļ�����
	 */
	public static void writeFile(String buildDir,String filePath,String remoteFilePath) {
		mkdir(buildDir);
		BufferedReader bufReader = null;
		BufferedWriter bufWriter = null;
		try {
			bufReader = new BufferedReader(new FileReader(remoteFilePath));
			bufWriter = new BufferedWriter(new FileWriter(filePath));
			String temp = null;
			while ((temp = bufReader.readLine()) != null) {
				bufWriter.write(temp + "\n");
				System.out.println("bufWriter : " + temp);
				bufWriter.flush();
			}
		} catch (Exception e) {
			e.getStackTrace();
		} finally {
			if (bufReader != null && bufWriter != null) {
				try {
					bufReader.close();
					bufWriter.close();
				} catch (IOException e) {
					e.getStackTrace();
				}
			}
		}
	}

	/**
	 * ɾ���ļ��������ǵ����ļ����ļ���
	 * 
	 * @param fileName
	 *            ��ɾ�����ļ���
	 * @return �ļ�ɾ���ɹ�����true,���򷵻�false
	 */
	public static boolean delete(String fileName) {
		File file = new File(fileName);
		if (!file.exists()) {
			System.out.println("ɾ���ļ�ʧ�ܣ�" + fileName + "�ļ�������");
			return false;
		} else {
			if (file.isFile()) {

				return deleteFile(fileName);
			} else {
				return deleteDirectory(fileName);
			}
		}
	}

	/**
	 * ɾ�������ļ�
	 * 
	 * @param fileName
	 *            ��ɾ���ļ����ļ���
	 * @return �����ļ�ɾ���ɹ�����true,���򷵻�false
	 */
	public static boolean deleteFile(String fileName) {
		File file = new File(fileName);
		if (file.isFile() && file.exists()) {
			file.delete();
			System.out.println("ɾ�������ļ�" + fileName + "�ɹ���");
			return true;
		} else {
			System.out.println("ɾ�������ļ�" + fileName + "ʧ�ܣ�");
			return false;
		}
	}

	/**
	 * ɾ��Ŀ¼���ļ��У��Լ�Ŀ¼�µ��ļ�
	 * 
	 * @param dir
	 *            ��ɾ��Ŀ¼���ļ�·��
	 * @return Ŀ¼ɾ���ɹ�����true,���򷵻�false
	 */
	public static boolean deleteDirectory(String dir) {
		// ���dir�����ļ��ָ�����β���Զ�����ļ��ָ���
		if (!dir.endsWith(File.separator)) {
			dir = dir + File.separator;
		}
		File dirFile = new File(dir);
		// ���dir��Ӧ���ļ������ڣ����߲���һ��Ŀ¼�����˳�
		if (!dirFile.exists() || !dirFile.isDirectory()) {
			System.out.println("ɾ��Ŀ¼ʧ��" + dir + "Ŀ¼�����ڣ�");
			return false;
		}
		boolean flag = true;
		// ɾ���ļ����µ������ļ�(������Ŀ¼)
		File[] files = dirFile.listFiles();
		for (int i = 0; i < files.length; i++) {
			// ɾ�����ļ�
			if (files[i].isFile()) {
				flag = deleteFile(files[i].getAbsolutePath());
				if (!flag) {
					break;
				}
			}
			// ɾ����Ŀ¼
			else {
				flag = deleteDirectory(files[i].getAbsolutePath());
				if (!flag) {
					break;
				}
			}
		}
		if (!flag) {
			System.out.println("ɾ��Ŀ¼ʧ��");
			return false;
		}
		// ɾ����ǰĿ¼
		if (dirFile.delete()) {
			System.out.println("ɾ��Ŀ¼" + dir + "�ɹ���");
			return true;
		} else {
			System.out.println("ɾ��Ŀ¼" + dir + "ʧ�ܣ�");
			return false;
		}
	}

	/**
	 * ���ַ�Ϊ��λ��д�ļ�����
	 * 
	 * @param filePath
	 */
	public static void readFileByCharacter(String filePath) {
		File file = new File(filePath);
		// FileReader:������ȡ�ַ��ļ��ı���ࡣ
		FileReader reader = null;
		FileWriter writer = null;
		try {
			reader = new FileReader(file);
			writer = new FileWriter("d:/work/readFileByCharacter.txt");
			int temp;
			while ((temp = reader.read()) != -1) {
				writer.write((char) temp);
			}
		} catch (IOException e) {
			e.getStackTrace();
		} finally {
			if (reader != null && writer != null) {
				try {
					reader.close();
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * ����Ϊ��λ��д�ļ�����
	 * 
	 * @param filePath
	 */
	public static void readFileByLine(String filePath) {
		File file = new File(filePath);
		// BufferedReader:���ַ��������ж�ȡ�ı�����������ַ����Ӷ�ʵ���ַ���������еĸ�Ч��ȡ��
		BufferedReader bufReader = null;
		BufferedWriter bufWriter = null;
		try {
			// FileReader:������ȡ�ַ��ļ��ı���ࡣ
			bufReader = new BufferedReader(new FileReader(file));
			bufWriter = new BufferedWriter(new FileWriter(
					"d:/work/readFileByLine.txt"));
			// buf = new BufferedReader(new InputStreamReader(new
			// FileInputStream(file)));
			String temp = null;
			while ((temp = bufReader.readLine()) != null) {
				bufWriter.write(temp + "\n");
			}
		} catch (Exception e) {
			e.getStackTrace();
		} finally {
			if (bufReader != null && bufWriter != null) {
				try {
					bufReader.close();
					bufWriter.close();
				} catch (IOException e) {
					e.getStackTrace();
				}
			}
		}
	}

	/**
	 * ʹ��Java.nio ByteBuffer�ֽڽ�һ���ļ��������һ�ļ�
	 * 
	 * @param filePath
	 */
	public static void readFileByBybeBuffer(String filePath) {
		FileInputStream in = null;
		FileOutputStream out = null;
		try {
			// ��ȡԴ�ļ���Ŀ���ļ������������
			in = new FileInputStream(filePath);
			out = new FileOutputStream("d:/work/readFileByBybeBuffer.txt");
			// ��ȡ�������ͨ��
			FileChannel fcIn = in.getChannel();
			FileChannel fcOut = out.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(1024);
			while (true) {
				// clear�������軺������ʹ�����Խ��ܶ��������
				buffer.clear();
				// ������ͨ���н����ݶ���������
				int r = fcIn.read(buffer);
				if (r == -1) {
					break;
				}
				// flip�����û��������Խ��¶��������д����һ��ͨ��
				buffer.flip();
				fcOut.write(buffer);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (in != null && out != null) {
				try {
					in.close();
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static long getTime() {
		return System.currentTimeMillis();
	}

	public static void main(String args[]) {
		long time1 = getTime();
		// readFileByByte(FILE_PATH);// 8734,8281,8,7781,8047
		// readFileByCharacter(FILE_PATH);// 734, 437, 437, 438, 422
		// readFileByLine(FILE_PATH);// 110, 94, 94, 110, 93
		readFileByBybeBuffer(FILE_PATH);// 125, 78, 62, 78, 62
		long time2 = getTime();
		System.out.println(time2 - time1);
	}
}