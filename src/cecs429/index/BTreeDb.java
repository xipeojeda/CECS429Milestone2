package cecs429.index;

import cecs429.gui.GUI;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

public class BTreeDb {
    private String filePath;
    private String fileName;
    private DB db;
    private BTreeMap<String, Long> map;

    public BTreeDb(String filePath, String fileName){
        this.filePath = filePath;
        this.fileName = fileName;
        
   }

    public void writeToDb(String term, Long position){
        this.map = this.db.treeMap(this.fileName).keySerializer(Serializer.STRING).valueSerializer(Serializer.LONG).counterEnable().createOrOpen();
        this.map.put(term, position);
    }
    
    public void makeDb() {
    	this.db =  DBMaker.fileDB(this.filePath + this.fileName).make();
    
    }
    
    public Long getPosition(String term) {
    	return this.map.get(term);
    }
    
    public DB getDB() {
    	return this.db;
    }
    
    public void close() {
    	this.db.close();
    }
}
