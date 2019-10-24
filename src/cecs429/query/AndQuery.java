package cecs429.query;

import cecs429.index.Index;
import cecs429.index.Posting;
import cecs429.text.Normalize;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An AndQuery composes other QueryComponents and merges their postings in an intersection-like operation.
 */
public class AndQuery implements QueryComponent {
	private List<QueryComponent> mComponents;
	
	public AndQuery(List<QueryComponent> components) {
		mComponents = components;
	}
	
	@Override
	public List<Posting> getPostings(Index index, Normalize normalize) {
		List<Posting> result = new ArrayList<>();
		List<Posting> temp;

		// TODO: program the merge for an AndQuery, by gathering the postings of the composed QueryComponents and
		// intersecting the resulting postings.
		if (mComponents.size() == 0)  //check if mComponents list is empty
			return result;
		else
			result.addAll(mComponents.get(0).getPostings(index, normalize)); //Get postings of first index of "mComponents"

		//gather posting of QueryComponents and intersect with results list postings
		for (int i = 0; i < mComponents.size(); i++) {
			temp = mComponents.get(i).getPostings(index, normalize); //"AND" algorithm happens here
			result = intersection(result, temp); //CALL HELPER METHOD
		}

		return result;
	}

	/**
	 * Performs the intersection of two posting lists
	 * @param p1 List<Posting> associated with first query term
	 * @param p2 List<Posting> associated with following query terms
	 * @return A List<Posting> containing the overlapping (same) documentId(s)
	 */
	public List<Posting> intersection(List<Posting> p1, List<Posting> p2){
		List<Posting> answer = new ArrayList<>();

		int i = 0, j = 0;
		Posting post1, post2;
		while(i < p1.size() && j < p2.size()){
			post1 = p1.get(i);
			post2 = p2.get(j);
			//if documents id are equivalent add them both to list and increment both positions
			if(post1.getDocumentId() == post2.getDocumentId()) {
				answer.add(post1);
				i++;
				j++;
			}
			//if post 1 list docID at that position is less than post2 increment post1 list to next position
			else if(post1.getDocumentId() < post2.getDocumentId())
				i++;
				//if post 2 list docID at that position is less than post1 increment post2 list to next position
			else
				j++;
		}

		return answer;
	}

	@Override
	public String toString() {
		return
		 String.join(" ", mComponents.stream().map(c -> c.toString()).collect(Collectors.toList()));
	}
}
