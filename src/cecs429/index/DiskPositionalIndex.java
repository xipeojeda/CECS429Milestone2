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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class DiskPositionalIndex implements Index {
	private String mPath;
	private RandomAccessFile mVocabList;
	private RandomAccessFile mPostings;
	private RandomAccessFile docWeights;
	private long[] mVocabTable;
	private List<String> mFileNames;
	private List<String> terms = null;
	//private ConcurrentNavigableMap<String, Long> vocabDB;
	private ConcurrentNavigableMap<String, Long> postingsDB;
	private DB db;
	
	
	public DiskPositionalIndex(String path) {
		try {
			
			mPath = path + "index/";
			String mapDbPath = mPath.replace("\\", "\\\\");
			String temp = mapDbPath + "\\";
			mVocabList = new RandomAccessFile(new File(mPath, "vocab.bin"), "r");
			mPostings = new RandomAccessFile(new File(mPath, "postings.bin"), "r");
			db = DBMaker.fileDB(temp + "postingsTree").make();
			postingsDB = this.db.treeMap("postingsTree", Serializer.STRING, Serializer.LONG).open();
			
			mVocabTable = readVocabTable(mPath);
			mFileNames = readFileNames(mPath);
			docWeights = new RandomAccessFile(new File(mPath, "docWeights.bin"), "r");
		}catch(FileNotFoundException e) {
			System.out.println(e.toString());
		}
	}

	public List<Posting> getPostings(String term, boolean positions) {
    		long position = postingsDB.get(term);
    		if(position >= 0) {
    			if(positions == true)
    				return readPositionalPosting(mPostings, position);
    			else
    				return readPostingsBin(mPostings, position);
    		}
        	return null;
    }
	/*
	 * 
	 */

    public List<Posting> readPostingsBin(RandomAccessFile postings, long pos){
    	try {
    		//seek to the position where postings start
			postings.seek(pos);
			
			// read the 8 bytes for doc freq
			byte[] buffer = new byte[8];
			postings.read(buffer, 0, buffer.length);
			
			//use ByteBuffer to convert the 8 bytes to int
			int docFreq = ByteBuffer.wrap(buffer).getInt();

			int lastDocID = 0;

			ArrayList<Posting> posList = new ArrayList<>();
			
			for(int i = 0; i < docFreq; i++) {
				byte[] docBuffer = new byte[8];
				postings.read(docBuffer,0,docBuffer.length);
				int docID = ByteBuffer.wrap(docBuffer).getInt() + lastDocID;
				lastDocID = docID;

				byte[] tfBuffer = new byte[8];
				postings.read(tfBuffer, 0, tfBuffer.length);

				int termFreq = ByteBuffer.wrap(tfBuffer).getInt();
				postings.skipBytes(termFreq * 4);

				Posting post = new Posting(docID, termFreq);
				posList.add(post);
			}
			
			return posList;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
		return null;
    }

    public List<Posting> readPositionalPosting(RandomAccessFile postings, long position){
		try{
			postings.seek(position);
			byte[] buffer = new byte[8];
			postings.read(buffer,0,buffer.length);

			int documentFreq = ByteBuffer.wrap(buffer).getInt();

			int lastDocID = 0;
			int lastPosID = 0;
			ArrayList<Posting> posList = new ArrayList<>();

			for(int i = 0; i < documentFreq; i++){
				ArrayList<Integer> positions = new ArrayList<>();
				byte[] docBuffer = new byte[8];
				postings.read(docBuffer, 0, docBuffer.length);

				int docID = ByteBuffer.wrap(docBuffer).getInt() + lastDocID;
				lastDocID = docID;

				byte[] tfBuffer = new byte[8];
				postings.read(tfBuffer, 0, tfBuffer.length);

				int termFreq = ByteBuffer.wrap(tfBuffer).getInt();

				for(int j = 0; j < termFreq; j++){
					byte[] posBuffer = new byte[8];
					postings.read(posBuffer, 0, posBuffer.length);
					int pos = ByteBuffer.wrap(posBuffer).getInt() + lastPosID;
					lastPosID = pos;
					positions.add(pos);
				}

				Posting post = new Posting(docID, termFreq, positions);
				posList.add(post);
			}

			return  posList;
		}catch(IOException e){
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
			
			byte[] byteBuffer = new byte[8];
			tableFile.read(byteBuffer, 0, byteBuffer.length);
			
			int tableIndex = 0;
			vocabTable = new long[ByteBuffer.wrap(byteBuffer).getInt() * 2];
			byteBuffer = new byte[8];
			//while we keep reading 8 bytes
			while(tableFile.read(byteBuffer,0,byteBuffer.length) > 0) {
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
	/*
	 * Walk file tree to get file names
	 * @param path Directory path
	 * @return arraylist of file names
	 */
    private List<String> readFileNames(String path) {
		// TODO Auto-generated method stub
		List<String> fileNames = new ArrayList<>();
		String modPath = path.replace("index/","");
		try {
			Files.walkFileTree(Paths.get(modPath), new SimpleFileVisitor<Path>(){
				@Override
				public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
					return FileVisitResult.CONTINUE;
				}
				
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws FileNotFoundException{
					//only process json files
					if(file.toString().endsWith(".json"))
						fileNames.add(file.toFile().getName());//add to list
					else if (file.toString().endsWith(".txt"))
						fileNames.add(file.toFile().getName());
					return FileVisitResult.CONTINUE;
				}
				// We dont want to throw exceptions if files are locked
				//or other errors occur
				@Override
				public FileVisitResult visitFileFailed(Path file, IOException e) {
					return FileVisitResult.CONTINUE;
				}
			});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileNames;
	}
	
	public List<String> getFileNames(){
		return mFileNames;
	}
	
	public String[] getVocab() {
		List<String> vocabList = new ArrayList<>();
		int i = 0;
		int j = mVocabTable.length /2 - 1;
		while(i <= j) {
			try {
				
			
			int termLength;
			if(i == j) {
				termLength = (int) (mVocabList.length() - mVocabTable[i * 2]);
			}
			else {
				termLength = (int) (mVocabList.length() - mVocabTable[i * 2]);
			}
			
			byte[] buffer = new byte[termLength];
			mVocabList.read(buffer, 0, termLength);
			String term = new String(buffer, "ASCII");
			vocabList.add(term);
			}catch(IOException e) {
				e.printStackTrace();
			i++;
			}
		}
		return vocabList.toArray(new String[0]);
	}

	@Override
	public List<Posting> getPostings(String term) {
		// TODO Auto-generated method stub
		return null;
	}

	public int getDocumentCount(){
		return mFileNames.size();
	}

	private double findDocWeight(int documentID,
								 RandomAccessFile docWeightsFile, int offset) {
		try {
			docWeightsFile.seek(documentID*32+offset);
			byte[] buffer = new byte[8];
			docWeightsFile.read(buffer, 0, buffer.length);

			return ByteBuffer.wrap(buffer).getDouble();
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		return 0.0;
	}

	public double getDocWeight(int docID) {
    	return findDocWeight(docID, docWeights, 0);
	}

	public double getDocLength(int documentID) {
		return findDocWeight(documentID, docWeights, 8);
	}

	public double getDocByteSize(int documentID) {
		return findDocWeight(documentID, docWeights, 16);
	}

	public double getAverageTermFreq(int documentID) {
		return findDocWeight(documentID, docWeights, 24);
	}

	public double getAverageDocLength() {
		return findDocWeight(mFileNames.size(), docWeights, 0);
	}
}
