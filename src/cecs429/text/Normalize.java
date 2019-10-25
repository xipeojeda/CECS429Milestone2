package cecs429.text;

import cecs429.stemmer.englishStemmer;

import java.util.*;

public class Normalize implements TokenProcessor
{
	private String language;

	public Normalize(String language) {
		this.language = language;
	}

	public List<String> processToken(String token) {
		String newTokens = token;
		List<String> tokens = new ArrayList();// Set a token list to be returned later
		englishStemmer stemmer= new englishStemmer();

		//Block checks whether a token has a non alphanumeric at the beginning or end of the token and removes them
		if (!Character.isLetterOrDigit(token.charAt(0)) || !Character.isLetterOrDigit(token.charAt(token.length()-1)))  {
			newTokens = token.replaceAll("(^[\\W_]*)|([\\W_]*$)", "");
		}
		//System.out.println(newTokens);

		//Block removes the quotation marks and single quotes of the token
		if (token.contains("'") || tokens.contains("\"")) {
			newTokens = newTokens.replaceAll("'","");
			newTokens = newTokens.replaceAll("\"","");
		}

		if (token.contains("-")) { //Removes the hyphens
			String[] temp = null;
			temp = token.split("[,?.-]+");

			List<String> temp2 = new ArrayList<String>(Arrays.asList(temp));
			tokens = new ArrayList<String>(temp2);
			newTokens = newTokens.replaceAll("\\W", "");//Needed so the final token doesn't contain an non alphanumeric
			newTokens = newTokens.replaceAll("-","");
		}
		tokens.add(newTokens); //Adds the combined hyphen token and adds it to list

		for (int i=0;i < tokens.size(); i++) {//Block that lowercases and stems.
			tokens.set(i, tokens.get(i).toLowerCase());
			stemmer.setCurrent(tokens.get(i));
			stemmer.stem();
			tokens.set(i, stemmer.getCurrent());
		}
		//System.out.println(tokens);
		return tokens;
	}
}

