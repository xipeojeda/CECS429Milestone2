package cecs429.index;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class  DiskIndexWriter {
	private String folderPath;
	private Index index;
	

	public DiskIndexWriter() {
		// TODO Auto-generated constructor stub
	}
	/*
	 * Construct an DiskIndexWriter object
	 * also creates folder with path of corpus
	 * @param folderPath Folder where to write Index
	 * @param index 
	 */
	public DiskIndexWriter(String folderPath, Index index) {
		this.setIndex(index);
		this.setFolderPath(folderPath + "\\index");
		//creating directory
		File directory = new File(getFolderPath());
		if(!directory.exists()) {
			try {
				directory.mkdir();
			}catch(SecurityException e) {
				e.printStackTrace();
			}
		}
		
		
	}
	//Interface for sub-methods
	public interface WriteIndexInterface{
		List<Long> createPostingBin(String path, Index index);
		List<Long> createVocabBin(String path, Index index);
		void createVocabTable(String path, List<Long> vPos, List<Long> pPos);
	}
	
	/*
	 * Using to break down into 3 sub-methods
	 */
	public void writeIndex(){
		WriteIndexInterface wii = new WriteIndexInterface() {
			/*
			 * Returns and fills a list<long> that helps remember the byte position
			 * (long) of where each term in the vocab begins in the postings file
			 * @param - String path
			 * @param - Index index
			 * @return - List<Long>
			 */
			@Override
			public List<Long> createPostingBin(String path, Index index) {
				//holds the posting positions
				List<Long> postingPositions = new ArrayList<Long>();
				
				//keep track of current Position
				long currentPos = 0;
				DataOutputStream postingsBin = null;
				
				try {
					//creating posting.bin in folder path
					File file = new File(path, "postings.bin");
				
					postingsBin = new DataOutputStream(new FileOutputStream(file));
					
					//going through vocabulary
					for(String term: index.getVocabulary()) {
						//adding current positions to posting positions
						postingPositions.add(currentPos);
						//creating array list of postings from index
						List<Posting> postings = index.getPostings(term);
						//writing size to postings.bin 
						postingsBin.writeLong(postings.size());//document frequency
						//8 bytes per posting
						currentPos += 8;
						//loop through postings to get docID gap
						int lastDocID = 0;
						for(Posting post: postings) {
							int idGap = post.getDocumentId() - lastDocID;
							lastDocID = post.getDocumentId();
							postingsBin.writeInt(idGap);
							
						//looping through positions to get position gap
							int lastPosition = 0;
							for(int position: post.getPositions()) {
								int posGap = position - lastPosition;
								lastPosition = position;
								postingsBin.writeInt(posGap);
								currentPos += 8;
							}
						}
					}
					postingsBin.close();
				}
				catch(FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return postingPositions;
			
			}
			/*
			 * Write each term in the index vocabulary, one after another,
			 * to vocab.bin, should return/fill in the byte positions of the beginning
			 * of each term in that file. 
			 * @param - String path
			 * @param - Index index
			 * @return - List<Long>
			 */
			@Override
			public List<Long> createVocabBin(String path, Index index) {
				//array list to hold vocab positions
				List<Long> vocabPositions = new ArrayList<Long>();
				//keeps track of current position
				long currentPos = 0;
				DataOutputStream vocabBin = null;
				try {
					File vFile = new File(path, "vocab.txt");
					//Get the correct directory path (Windows format)
					String truePath = vFile.getParent();
					truePath.replace("\\", "\\\\");
					String temp = truePath + "\\";
					
					BTreeDb vocabTree = new BTreeDb(temp, "vocabTree"); //MOTHA TREE
					
					//create vocab.bin in folder path (saving as txt as per instructions say we can do) encoded in UTF-8
					vocabBin = new DataOutputStream(new FileOutputStream(vFile));
					//loop through vocabulary
					for(String term: index.getVocabulary()) {
						//gets bytes from each term and add them to byte[]
						byte[] utf8 = term.getBytes("UTF8");
						//write to vocab.bin
						vocabBin.write(utf8);
						//add current position to vocabPositions
						vocabPositions.add(currentPos);
						//increment current position from current terms utf8 length 
						vocabTree.writeToDb(term, currentPos);
						currentPos += utf8.length;
						
						System.out.println(vocabTree.getPosition("whale"));
						
					}
					vocabTree.close();
					vocabBin.close();
					
				}catch(FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return vocabPositions;

			}
			/*
			 * Builds vocabTable.bin, by writing the two long values associated with each term 
			 * from the other two methods. 
			 * @param - String path
			 * @param - List<Long> vPos
			 * @param - List<Long> pPos
			 * @return - void
			 */
			@Override
			public void createVocabTable(String path, List<Long> vPos, List<Long> pPos) {
				DataOutputStream vocabTable = null;
				try {
					//creating vocabTable.bin file in path
					vocabTable = new DataOutputStream(new FileOutputStream(new File(path, "vocabTable.bin")));
					//looping through vPos and writing two long values associated 
					//with each term into vocabTable.bin
					for(int i = 0; i < vPos.size(); i++) {
						long vPositions = vPos.get(i);
						long pPositions = pPos.get(i);
						//write vPositions and pPositions for each term to vocabTable
						vocabTable.writeLong(vPositions);
						vocabTable.writeLong(pPositions);
					}
					vocabTable.close();//closing stream
				}
				
				catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}	
		};
		List<Long> vocabPositions = wii.createVocabBin(getFolderPath(), getIndex());
		List<Long> postingPositions = wii.createPostingBin(getFolderPath(), getIndex());
		wii.createVocabTable(getFolderPath(), vocabPositions, postingPositions);
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


