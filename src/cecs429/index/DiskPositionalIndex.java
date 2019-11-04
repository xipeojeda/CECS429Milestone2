package cecs429.index;

import org.jetbrains.annotations.TestOnly;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.util.List;

public class DiskPositionalIndex implements Index {

    @Override
    public List<Posting> getPostings(String term) {
        return null;
    }

    @Override
    public List<String> getVocabulary() {
        return null;
    }
}
