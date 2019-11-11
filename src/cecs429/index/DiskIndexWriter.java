package cecs429.index;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import cecs429.documents.JsonFileDocument;
import cecs429.text.EnglishTokenStream;
import cecs429.text.Normalize;

public class  DiskIndexWriter {
	private String folderPath;
	private PositionalInvertedIndex index;
	private List<Map<String, Integer>> docTermFrequency; // term frequencies for a document
	private ArrayList<Integer> docLength;
	private List<Double> docByteSize; // byte size of each document
	private int corpusSize; 
	
	/*
	 * Construct an DiskIndexWriter object
	 * also creates folder with path of corpus
	 * @param folderPath Folder where to write Index
	 * @param index 
	 */
	public DiskIndexWriter(String folderPath, PositionalInvertedIndex index) {
		this.setIndex(index);
		this.setFolderPath(folderPath + "index");
		//creating directory
		File directory = new File(getFolderPath());
		if(!directory.exists()) {
			try {
				directory.mkdir();
			}catch(SecurityException e) {
				e.printStackTrace();
			}
		}
        docTermFrequency = new ArrayList<Map<String, Integer>>();
        docLength = new ArrayList<Integer>();
        docByteSize = new ArrayList<Double>();
		
	}
	
	public void buildIndex() {
		SortedSet<String> vocabulary = new TreeSet<>();
		indexFile(Paths.get(getFolderPath()), vocabulary, index);
		buildIndexForDirectory(index, getFolderPath());
		buildCorpusSizeFile(getFolderPath());
		buildWeight(getFolderPath());
	}
	
	private void buildWeight(String folderPath) {
		// TODO Auto-generated method stub
		FileOutputStream weightFile = null;
		
		try {
			weightFile = new FileOutputStream(new File(folderPath, "docWeights.bin"));
			for(int docID = 0; docID < docTermFrequency.size(); docID++) {
				double docWeight = 0; //Ld
				double avgTermFrequency = 0;
				
				for(Integer tf: docTermFrequency.get(docID).values()) {
					double termWeight = 1 + (Math.log(tf));
					docWeight += Math.pow(termWeight, 2);
					avgTermFrequency += tf;
				}
				
				docWeight = Math.sqrt(docWeight);
				byte[] docWeightByte = ByteBuffer.allocate(8).putDouble(docWeight).array();
				
				weightFile.write(docWeightByte, 0, docWeightByte.length);
					
				avgTermFrequency /= docTermFrequency.get(docID).keySet().size();
				byte[] avgtfByte = ByteBuffer.allocate(8).putDouble(avgTermFrequency).array();
				weightFile.write(avgtfByte, 0, avgtfByte.length);
			
				double avgDocLength = 0;
				for(int dLength: docLength) {
					avgDocLength += dLength;
				}
				avgDocLength /= corpusSize;
				byte[] avgDocLengthByte = ByteBuffer.allocate(8).putDouble(avgDocLength).array();
				weightFile.write(avgDocLengthByte, 0, avgDocLengthByte.length);
				
				weightFile.close();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			try {
				weightFile.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
		
	}

	private void buildCorpusSizeFile(String folderPath) {
		FileOutputStream corpusFile = null;
		
		try {
			corpusFile = new FileOutputStream(new File(folderPath, "corpusSize.bin"));
			byte[] cSize = ByteBuffer.allocate(4).putInt(corpusSize).array();
			corpusFile.write(cSize);
			corpusFile.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void buildIndexForDirectory(PositionalInvertedIndex index, String folderPath) {
		long[] vPos = new long[index.getVocabulary().size()];
		buildVocabFile(folderPath, index.getVocabulary(), vPos, "vocab.bin");
		buildPostingFile(folderPath, index, index.getVocabulary(), vPos);
	}
	
	private static void buildPostingFile(String folderPath, PositionalInvertedIndex index, List<String> vocab, long[] vPos) {
		FileOutputStream postingsFile = null;
		try {
			postingsFile = new FileOutputStream(new File(folderPath, "postings.bin"));
			FileOutputStream vocabTable = new FileOutputStream(new File(folderPath, "vocabTable.bin"));
			
			byte[] tSize = ByteBuffer.allocate(4).putInt(vocab.size()).array();
			vocabTable.write(tSize, 0, tSize.length);
			
			int vocabIndex = 0;
			for(String s: vocab) {
				List<Posting> postings = index.getPostings(s);
				byte[] vPosBytes = ByteBuffer.allocate(8).putLong(vPos[vocabIndex]).array();
				vocabTable.write(vPosBytes, 0, vPosBytes.length);
				
				byte[] pPosBytes = ByteBuffer.allocate(8).putLong(postingsFile.getChannel().position()).array();
				vocabTable.write(pPosBytes,0, pPosBytes.length);
				
				byte[] docFreqBytes = ByteBuffer.allocate(4).putInt(postings.size()).array();
				postingsFile.write(docFreqBytes, 0, docFreqBytes.length);
				
				int lastDocID = 0;
				for(Posting p: postings) {
					byte[] docIDBytes = ByteBuffer.allocate(4).putInt(p.getDocumentId() - lastDocID).array();
					postingsFile.write(docIDBytes, 0, docIDBytes.length);
					lastDocID = p.getDocumentId();
					
					int termFrequency = p.getPositions().size();
					byte[] termFreqBytes = ByteBuffer.allocate(4).putInt(termFrequency).array();
					postingsFile.write(termFreqBytes, 0, termFreqBytes.length);
					
					int lastPos = 0;
					for(Integer pos: p.getPositions()) {
						byte[] positionBytes = ByteBuffer.allocate(4).putInt(pos - lastPos).array();
						postingsFile.write(positionBytes, 0, positionBytes.length);
						lastPos = pos;
					}
			}
				vocabIndex++;
			}
			vocabTable.close();
			postingsFile.close();
		}catch(FileNotFoundException e) {
			e.printStackTrace();
		}catch(IOException e) {
			e.printStackTrace();
		}finally {
			try {
				postingsFile.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static void buildVocabFile(String folderPath, List<String> vocab, long[] vPos, String file) {
		OutputStreamWriter vocabList = null;
		
		int vocabIndex = 0;
		try {
			vocabList = new OutputStreamWriter(new FileOutputStream(new File(folderPath, file)), "ASCII");
			
			int vocabPos = 0;
			for(String term: vocab) {
				vPos[vocabIndex] = vocabPos;
				try {
					vocabList.write(term);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				vocabIndex++;
				vocabPos += term.length();
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			try {
				vocabList.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	private void indexFile(Path path, SortedSet<String> vocab, PositionalInvertedIndex index) {
		// TODO Auto-generated method stub
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                int mDocumentID = 0;

                @Override
                public FileVisitResult preVisitDirectory(Path dir,
                        BasicFileAttributes attrs) {
                    // process the current working directory and subdirectories
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file,
                        BasicFileAttributes attrs) throws IOException {
                    // only process .json files
                    if (file.toString().endsWith(".json")) {
                        // get the number of bytes in the file and add to list
                        double size = file.toFile().length();
                        docByteSize.add(size);
                        // do the indexing
                        indexFile(file.toFile(), index, vocab,mDocumentID);
                        mDocumentID++;
                    }
                    return FileVisitResult.CONTINUE;
                }

				// don't throw exceptions if files are locked/other errors occur
                @Override
                public FileVisitResult visitFileFailed(Path file,
                        IOException e) {

                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException ex) {
            System.out.println(ex.toString());
        }
	}
	
	 private int indexFile(File file, PositionalInvertedIndex index, SortedSet<String> vocab, int docID) {
		List<String> terms;
		try {
			Gson gson = new Gson();
			JsonFileDocument doc;
			
			vocab = new TreeSet<>();
			docTermFrequency.add(new HashMap<String, Integer>());
			JsonReader reader = new JsonReader(new FileReader(file));
			doc = gson.fromJson(reader, JsonFileDocument.class);
			EnglishTokenStream ets = new EnglishTokenStream(doc.getContent());
			Normalize normal = new Normalize("en");
			int position = 0;
			for(String str :ets.getTokens()) {
				terms = normal.processToken(str);
				
				for(String term: terms) {
					vocab.add(term);
					index.addTerm(term, docID, position);
					int termFrequency = docTermFrequency.get(docID).containsKey(term) ? docTermFrequency.get(docID).get(terms): 0;
					docTermFrequency.get(docID).put(term, termFrequency + 1);
					position++;
				}
			}
			corpusSize++;
			
			docLength.add(position);
			
			
			

		}catch(FileNotFoundException e) {
			e.printStackTrace();
		}
		
		return corpusSize;
		 
	 }
	 
	 

	//setters and getters for instance variables
	public Index getIndex() {
		return index;
	}
	public void setIndex(PositionalInvertedIndex index) {
		this.index = index;
	}
	public String getFolderPath() {
		return folderPath;
	}
	public void setFolderPath(String folderPath) {
		this.folderPath = folderPath;
	}

}


