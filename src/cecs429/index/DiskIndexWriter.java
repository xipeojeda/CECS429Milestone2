package cecs429.index;

import java.io.*;
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

import cecs429.documents.TextFileDocument;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;

import cecs429.documents.JsonFileDocument;
import cecs429.text.EnglishTokenStream;
import cecs429.text.Normalize;

public class  DiskIndexWriter {
	private String folderPath;
	private Index index;
	private List<Map<String, Integer>> docTermFrequency; // term frequencies for a document
	private ArrayList<Integer> docLength;
	private List<Double> docByteSize; // byte size of each document
	private int corpusSize; 
	private String lang;
	/*
	 * Construct an DiskIndexWriter object
	 * also creates folder with path of corpus
	 * @param folderPath Folder where to write Index
	 * @param index 
	 */
	public DiskIndexWriter(String folderPath, Index index,String lang) {
		this.lang=lang;
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
	/*
	 * 
	 */
	public void buildIndex() {
		SortedSet<String> vocabulary = new TreeSet<>();
		indexFile(Paths.get(getFolderPath()), vocabulary, index);
		buildIndexForDirectory(index, getFolderPath());
		buildCorpusSizeFile(getFolderPath());
		buildWeight(getFolderPath());
	}
	/*
	 * 
	 */
	private void buildWeight(String folderPath) {
		// TODO Auto-generated method stub
		FileOutputStream weightFile = null;
		try {
			weightFile = new FileOutputStream(new File(folderPath, "docWeights.bin"));
			for(int docID = 0; docID < docTermFrequency.size(); docID++) {
				double docWeight = 0; //Ld
				double avgTermFrequency = 0;
				
				for(Integer tf: docTermFrequency.get(docID).values()) {
					double termWeight = 1 + (Math.log(tf)); //wdt
					docWeight += Math.pow(termWeight, 2);
					avgTermFrequency += tf;
				}
				//Writing wdt to file
				docWeight = Math.sqrt(docWeight);
				byte[] docWeightByte = ByteBuffer.allocate(8).putDouble(docWeight).array();
				weightFile.write(docWeightByte, 0, docWeightByte.length);
					
				// Write document length to file
				double length = docLength.get(docID);
				byte[] docLengthByte = ByteBuffer.allocate(8).putDouble(length).array();
				weightFile.write(docLengthByte, 0, docLengthByte.length);
				
				// Write document size to file
				double byteSize = docByteSize.get(docID);
				byte[] docSizeByte = ByteBuffer.allocate(8).putDouble(byteSize).array();
				weightFile.write(docSizeByte, 0, docSizeByte.length);
				
				//write avg tf count to file
				avgTermFrequency /= docTermFrequency.get(docID).keySet().size();
				byte[] avgtfByte = ByteBuffer.allocate(8).putDouble(avgTermFrequency).array();
				weightFile.write(avgtfByte, 0, avgtfByte.length);
			}
				double avgDocLength = 0;
				for(int dLength: docLength) {
					avgDocLength += dLength;
				}
				avgDocLength /= corpusSize;
				byte[] avgDocLengthByte = ByteBuffer.allocate(8).putDouble(avgDocLength).array();
				weightFile.write(avgDocLengthByte, 0, avgDocLengthByte.length);
				
			weightFile.close();
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
	/*
	 * 
	 */
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
	/*
	 * 
	 */
	private void buildIndexForDirectory(Index index, String folderPath) {
		long[] vPos = new long[index.getVocabulary().size()];
		buildVocabFile(folderPath, index.getVocabulary(), vPos, "vocab.bin");
		buildPostingFile(folderPath, index, index.getVocabulary(), vPos);
	}
	/*
	 * 
	 */
	private static void buildPostingFile(String folderPath, Index index, List<String> vocab, long[] vPos) {
		FileOutputStream postingsFile = null;
		try {
			File file = new File(folderPath, "postings.bin");
			
			String truePath = file.getParent();
			truePath.replace("\\", "\\\\");
			String temp = truePath + "\\";
			// Creating BTreeDb object
			BTreeDb postingsTree = new BTreeDb(temp, "postingsTree"); //MOTHA TREE
			// Making tree
			postingsTree.makeDb();
			postingsFile = new FileOutputStream(file);
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
				
				// Writing to db in form of string and long
				//term and pPosition ????
				postingsTree.writeToDb(s, postingsFile.getChannel().position());
				
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
			postingsTree.close();
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
	/*
	 * 
	 */
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
	/*
	 * 
	 */
	private void indexFile(Path path, SortedSet<String> vocab, Index index) {
		// TODO Auto-generated method stub
		String fullPath = path.toString();
		String modPath = fullPath.replace("index", "");
		Path newPath = Paths.get(modPath);
	
        try {
            Files.walkFileTree(newPath, new SimpleFileVisitor<Path>() {
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
                        indexFileTypes(file.toFile(), index, vocab, mDocumentID, "json");
                        mDocumentID++;
                    }
                    else if (file.toString().endsWith(".txt")) {
						// get the number of bytes in the file and add to list
						double size = file.toFile().length();
						docByteSize.add(size);
						// do the indexing
						indexFileTypes(file.toFile(), index, vocab, mDocumentID, "txt");
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
	/*
	 * 
	 */
	 private int indexFileTypes(File file, Index index, SortedSet<String> vocab, int docID, String fileType) {
		 List<String> terms;
		try {
			switch (fileType) {
				case "json":
					Gson gson = new Gson();
					JsonFileDocument doc;
					PositionalInvertedIndex pii = new PositionalInvertedIndex();
					vocab = new TreeSet<>();
					docTermFrequency.add(new HashMap<String, Integer>());
					JsonReader reader = new JsonReader(new FileReader(file));
					JsonObject jsonObject = gson.fromJson(reader, JsonObject.class);
					
					StringReader stringReader = new StringReader(jsonObject.get("body").toString());
					EnglishTokenStream ets = new EnglishTokenStream(stringReader);
					Normalize normal = new Normalize(lang); /////////////////////////////////////////////LANG
					int position = 0;
					for(String str : ets.getTokens()) {
						terms = normal.processToken(str);
						for(String term: terms) {
							vocab.add(term);
							pii.addTerm(term, docID, position);
							int termFrequency = docTermFrequency.get(docID).containsKey(term) ? docTermFrequency.get(docID).get(term): 0;
							docTermFrequency.get(docID).put(term, termFrequency + 1);
							position++;
						}
					}
					corpusSize++;
					docLength.add(position);
					break;
				case "txt":
					PositionalInvertedIndex piiTxt = new PositionalInvertedIndex();
					vocab = new TreeSet<>();
					docTermFrequency.add(new HashMap<String, Integer>());

					TextFileDocument txtDoc =  new TextFileDocument(docID, Paths.get(file.getAbsolutePath()));
					EnglishTokenStream etsTxt = new EnglishTokenStream(txtDoc.getContent());
					Normalize normalTxt = new Normalize(lang); /////////////////////////////////////////////LANG
					int positionTxt = 0;

					for(String str :etsTxt.getTokens()) {
						terms = normalTxt.processToken(str);

						for(String term: terms) {
							vocab.add(term);
							piiTxt.addTerm(term, docID, positionTxt);
							int termFrequency = docTermFrequency.get(docID).containsKey(term) ? docTermFrequency.get(docID).get(term): 0;
							docTermFrequency.get(docID).put(term, termFrequency + 1);
							positionTxt++;
						}
					}
					corpusSize++;
					docLength.add(positionTxt);
					break;
			}
		}catch(FileNotFoundException e) {
			e.printStackTrace();
		}
		
		return corpusSize;
		 
	 }
	 
	 

	//setters and getters for instance variables
	public Index getIndex() {
		return index;
	}
	public void setIndex(Index index) {
		this.index = index;
	}
	public String getFolderPath() {
		return folderPath;
	}
	public void setFolderPath(String folderPath) {
		this.folderPath = folderPath;
	}

}


