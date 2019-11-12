package cecs429.query;

import cecs429.index.Index;

public interface RankFormula {
    double getWqt(Index i, String term);
    double getWdt(Index i, String term, int docId);
    double getLd(int docId);
}
