package cecs429.query;

import cecs429.index.DiskIndexWriter;
import cecs429.index.DiskPositionalIndex;

import java.util.ArrayList;

public interface Ranking {
    public ArrayList<Accumalator> rankAlgorithm(String query, DiskPositionalIndex index,String lang);
}
