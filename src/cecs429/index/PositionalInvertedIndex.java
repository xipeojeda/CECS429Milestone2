package cecs429.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class PositionalInvertedIndex implements Index {
	private HashMap<String,List<Posting>> map;
	public PositionalInvertedIndex()
	{
		this.map =  new HashMap<String,List<Posting>>();
	}
	@Override
	public List<Posting> getPostings(String term) {
		if(map.containsKey(term))
			return map.get(term);
		else
			return new ArrayList<>();
	}

	@Override
	public List<String> getVocabulary() {
		List<String> sortedKeysList = new ArrayList<>(map.keySet());
		Collections.sort(sortedKeysList);
		return sortedKeysList;
	}
	/*
	 * add term to hashmap must run at O(1)
	 */
	public void addTerm(String term, int docID, Integer position){
        //if maps contains the term
	    if(map.containsKey(term)){
	    	List<Posting> pL = map.get(term);
	        List<Integer> posList;
	        //if terms docID is at the last posting in list add that position    
	        if((pL.get(pL.size()-1).getDocumentId() == docID)){
	            	pL.get(pL.size()-1).getPositions().add(position);
	        }
	        //if not, add the position of the term
	        else {
	            posList = new ArrayList<Integer>();
	            posList.add(position);
	            pL.add(new Posting(docID, posList));
	            }
	        //add to hashmap    
	        map.put(term, pL);
	     }else { //if term is not in hashmap add to hashmap and its position
	    	 List<Posting> pL = new ArrayList<>();
	         List<Integer> posList = new ArrayList<Integer>();
	         posList.add(position);
	         pL.add(new Posting(docID, posList));
	         map.put(term, pL);
	      }
	}

}
