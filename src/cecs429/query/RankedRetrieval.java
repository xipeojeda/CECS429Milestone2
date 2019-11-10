package cecs429.query;

import cecs429.index.DiskIndexWriter;
import cecs429.index.Index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class RankedRetrieval implements Ranking, RankFormula {

    @Override
    public ArrayList<Accumalator> rankAlgorithm(String query, DiskIndexWriter index) {
        HashMap<Integer, Double> accMap = new HashMap<>();
        ArrayList<Accumalator> results = new ArrayList<>();
        String[] tokens = query.split(" ");
        for (int i =0; i < tokens; i++)
        {
            ArrayList<Positional>
        }

        return null;
    }

    @Override
    public double getWqt(Index i, String term) {
        return 0;
    }

    @Override
    public double getWdt(Index i, String term, int docId) {
        return 0;
    }

    @Override
    public double getLd(int docId) {
        return 0;
    }

}
