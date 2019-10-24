package cecs429.index;

import java.util.List;

/**
 * A Posting encapulates a document ID associated with a search query component.
 */
public class Posting {
	private int mDocumentId;
	List<Integer> mPositions;
	private double mWdt;
	public Posting(int documentId,List<Integer> positions) {
		mDocumentId = documentId;
		mPositions = positions;
	}
	
	public Posting(int documentId, List<Integer> positions, double wdt) {
		super();
		mDocumentId = documentId;
		mPositions = positions;
		mWdt = wdt;
		
	}
	
	public int getDocumentId() {
		return mDocumentId;
	}
	public List<Integer> getPositions()
	{
		return mPositions;
	}
	public double getWdt() {
		return mWdt;
	}
}
