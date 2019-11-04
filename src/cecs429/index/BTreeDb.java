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

    public BTreeDb(String filePath, String fileName){
        this.filePath = filePath;
        this.fileName = fileName;
        db =  DBMaker.fileDB(this.filePath).make();
   }

    public void writeToDb(String term, Long position){
        BTreeMap<String, Long> map = db.treeMap(this.fileName).keySerializer(Serializer.STRING).valueSerializer(Serializer.LONG).counterEnable().createOrOpen();
        map.put(term, position);
    }
}
