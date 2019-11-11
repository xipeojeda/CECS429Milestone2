package cecs429.text;

import cecs429.stemmer.englishStemmer;
import cecs429.stemmer.frenchStemmer;

import java.util.*;

public class Normalize implements TokenProcessor
{
	private String language;

	public Normalize(String language) {
		this.language = language.toLowerCase();
	}

	public List<String> processToken(String token) {
		String newTokens = token;
		List<String> tokens = new ArrayList<>();// Set a token list to be returned later

		//Block checks whether a token has a non alphanumeric at the beginning or end of the token and removes them
		if (!Character.isLetterOrDigit(token.charAt(0)) || !Character.isLetterOrDigit(token.charAt(token.length()-1)))  {
			newTokens = token.replaceAll("(^[\\W_]*)|([\\W_]*$)", "");
		}
		//System.out.println(newTokens); //DEBUGGING

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

		/*
			Process each token with the correct language stemmer
		 */
		switch (this.language) {
			case "en":
				englishStemmer enStemmer= new englishStemmer();

				for (int i = 0; i < tokens.size(); i++) {//Block that lowercases and stems.
					tokens.set(i, tokens.get(i).toLowerCase());
					enStemmer.setCurrent(tokens.get(i));
					enStemmer.stem();
					tokens.set(i, enStemmer.getCurrent());
				}
				break;
			case "fr":
				frenchStemmer frStemmer= new frenchStemmer();

				for (int i = 0; i < tokens.size(); i++) {//Block that lowercases and stems.
					tokens.set(i, tokens.get(i).toLowerCase());
					frStemmer.setCurrent(tokens.get(i));
					frStemmer.stem();
					tokens.set(i, frStemmer.getCurrent());
				}
				break;
		}
		//System.out.println(tokens); //DEBUGGING
		return tokens;
	}
}

