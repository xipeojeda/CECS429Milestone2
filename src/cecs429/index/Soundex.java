package cecs429.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Soundex implements Index{
	private HashMap<String,List<Posting>> map;
	
	public Soundex() {
		this.map =  new HashMap<String,List<Posting>>();
	}

	/*
	 * 
	 */
	@Override
	public List<Posting> getPostings(String term) {
		if (map.containsKey(term))
			return map.get(toSoundex(term));
		else
			return new ArrayList<>();
	}

	@Override
	public List<String> getVocabulary() {
		// TODO Auto-generated method stub
		return null;
	}
	/*
	 * 
	 */
	public void addCode(String term, int docID) {
		String soundexHash = toSoundex(term);
		Posting temp = new Posting(docID, null);
		if(map.containsKey(soundexHash)) {
			List<Posting> pL = map.get(soundexHash);
			if(!(pL.get(pL.size()-1).getDocumentId() == docID))
				pL.add(temp);
		}
		else {
            List<Posting> pL = new ArrayList<>();
            pL.add(temp);
            map.put(soundexHash, pL);
		}
	}
	/*
	 * 
	 */
	public String toSoundex(String term) {
		System.out.println(term);
		char [] temp = term.toUpperCase().toCharArray();
		if(term.length() == 0) {
			System.out.println("test");
			return null;
		}
		String firstLetter = term.substring(0,1);
		//converts letters to codes
		for(int i = 0; i < temp.length; i++) {
			switch(temp[i]) {
			case 'B':
			case 'F':
			case 'P':
			case 'V':{
				temp[i] = '1';
				break;
			}
			case 'C':
			case 'G':
			case 'J':
			case 'K':
			case 'Q':
			case 'S':
			case 'X':
			case 'Z':{
				temp[i] = '2';
				break;
			}
			case 'D':
			case 'T':{
				temp[i] = '3';
				break;
			}
			case 'L': {
				temp[i] = '4';
				break;
			}
			case 'M':
			case 'N':{
				temp[i] = '5';
				break;
			}
			case 'R':{
				temp[i] = '6';
				break;
			}
			default:{
				temp[i] = '0';
				break;
			}
			}
		}
		
		
		//remove duplicates
		String output = "" + firstLetter;
		for(int i = 1; i < temp.length; i++) {
			if(temp[i] != temp[i-1] && temp[i] != '0') {
				output += temp[i];
			}
		}
		
		//padding with 0 or truncate
		output = output + "0000";
		return output.substring(0,4);
	}
	
	public List<Posting> getCodePosts(String code){
		return map.get(code);
	}

}
