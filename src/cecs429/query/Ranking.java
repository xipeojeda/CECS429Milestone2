package cecs429.query;

import cecs429.index.DiskIndexWriter;

import java.util.ArrayList;

public interface Ranking {
    public ArrayList<Accumalator> rankAlgorithm(String query, DiskIndexWriter index);
}
