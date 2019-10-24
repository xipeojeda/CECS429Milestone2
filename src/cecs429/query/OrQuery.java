package cecs429.query;

import cecs429.index.Index;
import cecs429.index.Posting;
import cecs429.text.Normalize;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An OrQuery composes other QueryComponents and merges their postings with a union-type operation.
 */
public class OrQuery implements QueryComponent {
	// The components of the Or query.
	private List<QueryComponent> mComponents;
	
	public OrQuery(List<QueryComponent> components) {
		mComponents = components;
	}
	
	@Override
	public List<Posting> getPostings(Index index, Normalize normalize) {
		List<Posting> result = new ArrayList<>();
		List<Posting> temp;
		
		// TODO: program the merge for an OrQuery, by gathering the postings of the composed QueryComponents and
		// unioning the resulting postings.
		if(mComponents.size() == 0)
			return result;
		else
			result.addAll(mComponents.get(0).getPostings(index, normalize));

		for(int i = 0; i < mComponents.size();i++) {
			temp = mComponents.get(i).getPostings(index, normalize); //"OR" algorithm happens here
			result = union(result,temp); //CALL HELPER METHOD
		}

		return result;
	}

	/**
	 * performs the union of two lists
	 * @param p1 List<Posting> associated with first query term
	 * @param p2 List<Posting> associated with following query terms
	 * @return A List<Posting> containing the Union of both lists
	 */
	public List<Posting> union(List<Posting> p1, List<Posting> p2){
		List<Posting> answer = new ArrayList<>();

		Posting post1, post2;

		int i = 0, j = 0;
		while(i < p1.size() && j < p2.size()) {
			post1 = p1.get(i);
			post2 = p2.get(j);
			//if post1 docID is greater, add post1 and increment pointer
			if(post1.getDocumentId() < post2.getDocumentId()){
				answer.add(post1);
				i++;
			}
			//if post2 docID is greater, add post2 and increment pointer
			else if(post1.getDocumentId() > post2.getDocumentId()){
				answer.add(post2);
				j++;
			}
			else {
				//if docIDs are equivalent add post to answers list and increment both pointers
				i++;
				answer.add(post1);
				j++;
			}
			//adding posts from remaining elements of the larger array
		}

		while(i < p1.size()) {
			answer.add(post1 = p1.get(i++));
		}
		while(j < p2.size()) {
			answer.add(post2 = p2.get(j++));
		}
		
		return answer;
	}
	
	@Override
	public String toString() {
		// Returns a string of the form "[SUBQUERY] + [SUBQUERY] + [SUBQUERY]"
		return "(" +
		 String.join(" + ", mComponents.stream().map(c -> c.toString()).collect(Collectors.toList()))
		 + " )";
	}
}
