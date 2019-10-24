package cecs429.query;

import cecs429.index.Index;
import cecs429.index.Posting;
import cecs429.text.Normalize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a phrase literal consisting of one or more terms that must occur in sequence.
 */
public class PhraseLiteral implements QueryComponent {
	// The list of individual terms in the phrase.
	private List<String> mTerms = new ArrayList<>();

	/**
	 * Constructs a PhraseLiteral with the given individual phrase terms.
	 */
	public PhraseLiteral(List<String> terms) {
		mTerms.addAll(terms);
	}

	/**
	 * Constructs a PhraseLiteral given a string with one or more individual terms separated by spaces.
	 */
	public PhraseLiteral(String terms) {
		mTerms.addAll(Arrays.asList(terms.split(" ")));
	}

	@Override
	public List<Posting> getPostings(Index index, Normalize normalize) {
		// TODO: program this method. Retrieve the postings for the individual terms in the phrase,
		// and positional merge them together.

		List<Posting> result = new ArrayList<>();
		List<Posting> temp = new ArrayList<>();
		List<String> processedList = new ArrayList<>();
		for(String s: mTerms) {
			processedList.addAll(normalize.processToken(s));

		}

		//System.out.println("YOOOOOOOO " + mTerms.size());

		if(mTerms.size() == 0)
			return result;
		else
			result.addAll(index.getPostings(processedList.get(0)));

		for(int i = 1; i < mTerms.size();i++) {
			temp = index.getPostings(processedList.get(i)); //"POSITIONAL MERGE" happens here
			result = positionalMerge(result,temp, i); //CALL HELPER METHOD
		}

		return result;
	}
	// <= and ==  FOR NEAR

	/**
	 * proximity intersection of posting lists p1, and p2.
	 * finds places where two terms appear within k (position) words of
	 * each other an returns a list of triples giving docID and the term
	 * position in p1 and p2.
	 *"nature park" is supposed to return 27 for testing
	 * @param p1 The list of postings from the first search term
	 * @param p2 The list of postings from the second search term
	 * @return list of triples giving docID and the term position in p1 and p2
	 */
	public List<Posting> positionalMerge(List<Posting> p1, List<Posting> p2, int position){
		List<Posting> answer = new ArrayList<>();
		Posting post1, post2;

		int i = 0, j = 0;
		while(i < p1.size() && j < p2.size()){ // When the postings list are not empty
			// Check each posting position in List<Posting>
			post1 = p1.get(i);
			post2 = p2.get(j);
			if(post1.getDocumentId() ==  post2.getDocumentId()) { // If both IDs are the same
				int m = 0, n = 0;
				List<Integer> listOfPositions = new ArrayList<>(); // Create a new list to hold the positions of terms within docID
				// Get position of the postings from each postings list
				List<Integer> pp1 = post1.getPositions();
				List<Integer> pp2 = post2.getPositions();

				while(m < pp1.size() && n < pp2.size()) { // Loop through the lists
					if (pp2.get(n) - pp1.get(m) == position) {
						listOfPositions.add(pp1.get(m));

						m++;
						n++;
					}
					else if(pp2.get(n) > pp1.get(m)) { // If n > m, break out of while loop
						m++;
					}
					else {
						n++;
					}
					/*while(!listOfPositions.isEmpty() && Math.abs(listOfPositions.get(0) - pp1.get(m)) > position) {
						listOfPositions.remove(listOfPositions.get(0)); //Remove the first index of listOfPositions
					}
					*/

				}
				if (!listOfPositions.isEmpty()) {
					answer.add(new Posting(post1.getDocumentId(), listOfPositions));
				}

				//If none of the conditions above are met, increment to next positions
				i++;
				j++;
			}
			else if(post1.getDocumentId() < post2.getDocumentId())
				i++;
			else
				j++;

		}

		return answer;
	}

	@Override
	public String toString() {
		return "\"" + String.join(" ", mTerms) + "\"";
	}
}
