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
        db =  DBMaker.fileDB(this.filePath + this.fileName).make();
   }

    public void writeToDb(String term, Long position){
        this.map = db.treeMap(this.fileName).keySerializer(Serializer.STRING).valueSerializer(Serializer.LONG).counterEnable().createOrOpen();
        this.map.put(term, position);
    }
    
    public Long getPosition(String term) {
    	return this.map.get(term);
    }
}
