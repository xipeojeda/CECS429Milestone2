package cecs429.query;

public class Accumalator {
    private int docID;
    private double accumulator;

    /*
	 * reads the file vocabTable.bin into memory
	 */
    public Accumalator(int docID, double accumulator)
    {
        this.docID = docID;
        this.accumulator = accumulator;
    }

    /*
	 * reads the file vocabTable.bin into memory
	 */
    public int getDocID() {
        return docID;
    }

    /*
	 * reads the file vocabTable.bin into memory
	 */
    public double getAccumulator() {
        return accumulator;
    }
}
