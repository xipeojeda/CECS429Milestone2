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
import java.util.stream.Collectors;

public class DiskPositionalIndex implements Index {
	private String mPath;
	private RandomAccessFile mVocabList;
	private RandomAccessFile mPostings;
	private RandomAccessFile docWeights;
	private long[] mVocabTable;
	private List<String> mFileNames;
	private List<String> terms = null;
	private ConcurrentNavigableMap<String, Long> vocabDB;
	private DB db;
	
	
	public DiskPositionalIndex(String path) {
		try {
			
			mPath = path;
			mVocabList = new RandomAccessFile(new File(path, "vocab.txt"), "r");
			mPostings = new RandomAccessFile(new File(path, "postings.bin"), "r");
			vocabDB = this.db.treeMap("vocabTree", Serializer.STRING, Serializer.LONG).open();
			mVocabTable = readVocabTable(path);
			mFileNames = readFileNames(path);
			docWeights = new RandomAccessFile(new File(path, "docWeights.bin"), "r");
		}catch(FileNotFoundException e) {
			System.out.println(e.toString());
		}
	}

	public List<Posting> getPostings(String term, boolean positions) {
    		long position = bTreeSearchVocab(term);
    		if(position >= 0) {
    			return readPostingsBin(mPostings, position);
    		}
        	return null;
    }
	
	private long bTreeSearchVocab(String term){
		long vListPosition = vocabDB.get(term);
	      // do a binary search over the vocabulary, using the vocabTable and the file vocabList.
	      int i = 0, j = mVocabTable.length / 2 - 1;
	      while (i <= j) {
	         try {
	        	  int m = (i + j) / 2;
	        	  int termLength;
	             if (m == mVocabTable.length / 2 - 1) {
	                 termLength = (int)(mVocabList.length() - mVocabTable[m*2]);
	              }
	              else {
	                 termLength = (int) (mVocabTable[(m + 1) * 2] - vListPosition);
	              }
	            mVocabList.seek(vListPosition);

	            byte[] buffer = new byte[termLength];
	            mVocabList.read(buffer, 0, termLength);
	            String fileTerm = new String(buffer, "ASCII");

	            int compareValue = term.compareTo(fileTerm);
	            if (compareValue == 0) {
	               // found it!
	               return mVocabTable[m * 2 + 1];
	            }
	            else if (compareValue < 0) {
	               j = m - 1;
	            }
	            else {
	               i = m + 1;
	            }
	         }
	         catch (IOException ex) {
	            System.out.println(ex.toString());
	         }
	      }
	      return -1;
	}
    
    public List<Posting> readPostingsBin(RandomAccessFile postings, long pos){
   
    	List<Posting> postingsList = new ArrayList<Posting>();
    	
    	
    	try {
    		//seek to the position where postings start
			postings.seek(pos);
			
			// read the 8 bytes for doc freq
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
		try {
			Files.walkFileTree(Paths.get(path), new SimpleFileVisitor<Path>(){
				@Override
				public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
					return FileVisitResult.CONTINUE;
				}
				
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws FileNotFoundException{
					//only process json files
					if(file.toString().endsWith(".json")) {
						fileNames.add(file.toFile().getName());//add to list
					}
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
}
