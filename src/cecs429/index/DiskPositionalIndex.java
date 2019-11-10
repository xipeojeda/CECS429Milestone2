package cecs429.index;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;

public class DiskPositionalIndex implements Index{
	
	private String mPath;
	private RandomAccessFile mVocabList;
	private RandomAccessFile mPostings;
	private long[] mVocabTable;
	
	//Opens a disk positional index that was constructed
	//in the given path
	public DiskPositionalIndex(String path) {
		try {
			mPath = path;
			mVocabList = new RandomAccessFile(new File(path, "vocab.txt"), "r");
			mPostings = new RandomAccessFile(new File(path, "postings.bin"), "r");
			mVocabTable = readVocabTable(path);
		}catch(FileNotFoundException e) {
			System.out.println(e.toString());
		}
	}

	/*
	 * returns postings for a given term
	 */
	@Override
	public List<Posting> getPostings(String term) {
		return null;
	}
	
	/*
	 * returns entire vocab
	 */
	@Override
	public List<String> getVocabulary() {
		// TODO Auto-generated method stub
		return null;
	}
	
	/*
	 * reads the file vocabTable.bin into memory
	 */
	private static long[] readVocabTable(String indexName) {
		try {
			long[] vocabTable;
			RandomAccessFile tableFile = new RandomAccessFile(new File(indexName, "vocabTable.bin"), "r");
			
			byte[] byteBuffer = new byte[4];
			tableFile.read(byteBuffer, 0, byteBuffer.length);
			
			int tableIndex = 0;
			vocabTable = new long[ByteBuffer.wrap(byteBuffer).getInt() * 2];
			byteBuffer = new byte[8];
			//while we keep reading 4 bytes
			while(tableFile.read(byteBuffer) > 0) {
				vocabTable[tableIndex] = ByteBuffer.wrap(byteBuffer).getLong();
				tableIndex++;
			}
			
			tableFile.close();
			return vocabTable;
		}catch(FileNotFoundException ex) {
			ex.printStackTrace();
		}
		catch(IOException ex){
			ex.printStackTrace();
		}
		return null;
	}
	
	public int getTermCount() {
		return mVocabTable.length / 2;
	}

}
