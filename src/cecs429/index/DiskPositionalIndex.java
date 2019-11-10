package cecs429.index;

import org.jetbrains.annotations.TestOnly;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.stream.Collectors;

public class DiskPositionalIndex implements Index {
	private String mPath;
	private RandomAccessFile mVocabList;
	private RandomAccessFile mPostings;
	private long[] mVocabTable;
	private ConcurrentNavigableMap<String, Long> postingsDB;
	private DB db;
	
	
	public DiskPositionalIndex(String path) {
		try {
			mPath = path;
			mVocabList = new RandomAccessFile(new File(path, "vocab.txt"), "r");
			mPostings = new RandomAccessFile(new File(path, "postings.bin"), "r");
			this.postingsDB = this.db.treeMap("vocabTree", Serializer.STRING, Serializer.LONG).open();
		}catch(FileNotFoundException e) {
			System.out.println(e.toString());
		}
	}
    @Override
    public List<Posting> getPostings(String term) {
    		long position = postingsDB.get(term);
    		if(position >= 0) {
    			return readPostingsBin(mPostings, position);
    		}
        	return null;
    }
    
    public List<Posting> readPostingsBin(RandomAccessFile postings, long pos){
   
    	List<Posting> postingsList = new ArrayList<Posting>();
    	
    	
    	try {
    		//seek to the position where postings start
			postings.seek(pos);
			
			// read the 4 bytes for doc freq
			byte[] buffer = new byte[8];
			postings.read(buffer, 0, buffer.length);
			
			//use ByteBuffer to convert the 4 bytes to int
			int docFreq = ByteBuffer.wrap(buffer).getInt();
			
			int docId = 0;
			int lDocId = 0;
			
			byte docIdsBuffer[] = new byte[8];
			byte positionBuffer[]= new byte[8];
			byte wdtBuffer[] = new byte[8];
			
			for(int docIdIndex = 0; docIdIndex < docFreq; docIdIndex++) {
				postings.read(docIdsBuffer,0,docIdsBuffer.length);
				docId = ByteBuffer.wrap(docIdsBuffer).getInt() + lDocId;
				
				postings.read(wdtBuffer, 0, wdtBuffer.length);
				double wdt = ByteBuffer.wrap(wdtBuffer).getDouble();
				buffer = new byte[8];
				
				postings.read(buffer, 0, buffer.length);
				int termFreq = ByteBuffer.wrap(buffer).getInt();
				
				int[] positions = new int[termFreq];
				
				for(int positionIndex = 0; positionIndex < termFreq; positionIndex++) {
					postings.read(positionBuffer, 0, positionBuffer.length);
					positions[positionIndex] = ByteBuffer.wrap(positionBuffer).getInt();
				}
				lDocId = docId;
				Posting post = new Posting(docId,Arrays.stream(positions).boxed().collect(Collectors.toList()), wdt);
				postingsList.add(post);
			}
			
			return postingsList;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
		return null;
    }

    @Override
    public List<String> getVocabulary() {
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
