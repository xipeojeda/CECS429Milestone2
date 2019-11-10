package cecs429.query;

/**
 *
 */
public class Accumalator {
    private int docID;
    private double accumulator;

    public Accumalator(int docID, double accumulator)
    {
        this.docID = docID;
        this.accumulator = accumulator;
    }

    public int getDocID() {
        return docID;
    }

    public double getAccumulator() {
        return accumulator;
    }
}
