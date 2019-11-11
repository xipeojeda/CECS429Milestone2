package cecs429.query;

import java.util.Comparator;

public class AccumulatorSort implements Comparator<Accumalator> {
    @Override
    public int compare(Accumalator a1, Accumalator a2){
        return Double.compare(a2.getAccumulator(), a1.getAccumulator());
    }
}
