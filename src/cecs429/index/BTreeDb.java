package cecs429.index;

import cecs429.gui.GUI;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

/**
 * A B+ tree structure to provide more efficient searching
 */
public class BTreeDb {
    private String filePath;
    private String fileName;
    private DB db;
    private BTreeMap<String, Long> map;

    /**
     * Overloaded constructor of BTreeDb
     * @param filePath the file path
     * @param fileName the name of the file
     */
    public BTreeDb(String filePath, String fileName){
        this.filePath = filePath;
        this.fileName = fileName;
   }

    /**
     * Maps a term (key) to the position in long (val)
     * @param term the term
     * @param position the position of the term in Long format 
     */
    public void writeToDb(String term, Long position){
        this.map = this.db.treeMap(this.fileName).keySerializer(Serializer.STRING).valueSerializer(Serializer.LONG).counterEnable().createOrOpen();
        this.map.put(term, position);
    }
    
    /**
     * Creates the internal DB structure required to support BTreeMap
     */
    public void makeDb() {
    	this.db =  DBMaker.fileDB(this.filePath + this.fileName).make();
    }
    
    /**
     * Returns the position associated with the term
     * @param term the term
     * @return the Long val associated with the term
     */
    public Long getPosition(String term) {
    	return this.map.get(term);
    }
    
    /**
     * @return the internal DB structure of the BTreeDb object
     */
    public DB getDB() {
    	return this.db;
    }
    
    /**
     * Closes the writing to the internal DB to avoid leaks
     */
    public void close() {
    	this.db.close();
    }
}
