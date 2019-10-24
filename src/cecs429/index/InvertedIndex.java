package cecs429.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class InvertedIndex implements Index{
	private HashMap<String, List<Posting>> map;
	
	//default constructor
	public InvertedIndex() 
	{
		// TODO Auto-generated constructor stub
		this.map = new HashMap<String, List<Posting>>();
		
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
	
	public void addTerm(String term, int docID){
        Posting temp = new Posting(docID, null);
        if(map.containsKey(term)){
            List<Posting> pL = map.get(term);

            if(!(pL.get(pL.size()-1).getDocumentId() == docID))
                pL.add(temp);
        }else {
            List<Posting> pL = new ArrayList<>();

            pL.add(temp);
            map.put(term, pL);
        }
	}

}
