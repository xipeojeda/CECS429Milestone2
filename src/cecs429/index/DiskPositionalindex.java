package cecs429.index;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.List;

public class DiskPositionalindex implements Index{
	
	private String mPath;
	private RandomAccessFile mVocabList;
	private RandomAccessFile mPostings;
	private long[] mVocabTable;
	
	//Opens a disk positional index that was constructed
	//in the given path
	public DiskPositionalindex(String path) {
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
	 * 
	 */
	@Override
	public List<Posting> getPostings(String term) {
		// TODO Auto-generated method stub
		return null;
	}
	
	/*
	 * Irvin will be handling this using B+ Trees
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
		return null;
	}

}
