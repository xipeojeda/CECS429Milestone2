package cecs429.text;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.lang.Character;
import cecs429.stemmer.*;

public class Normalize implements TokenProcessor
{
	public Normalize()
	{
		
	}

	public List<String> processToken(String token) {
		String newtokes=token;
		List<String> tokens = new ArrayList();// Set a token list to be returned later
		 englishStemmer stemmer= new englishStemmer();
		 String stemmed = null;
		if(!Character.isLetterOrDigit(token.charAt(0))||!Character.isLetterOrDigit(token.charAt(token.length()-1)))
		{//Block checks whether a token has a non alphanumeric at the beginning or end of the token and removes them
		newtokes=token.replaceAll("\\W", "");
		}
		if(token.contains("'")||tokens.contains("\""))
		{
		//Block removes the quotation marks and single quotes of the token
		newtokes=newtokes.replaceAll("'","");
		newtokes=newtokes.replaceAll("\"","");
		}
		if(token.contains("-"))// Removes the hyphens
			{
				String tempstr = newtokes;
			String[] temp = null;
			temp=tempstr.split("[,?.-]+");
		
			List<String> temp2 = new ArrayList<String>(Arrays.asList(temp));
			tokens = new ArrayList<String>(temp2);
			newtokes=newtokes.replaceAll("\\W", "");//Needed so the final token doesn't contain an non alphanumeric
			newtokes=newtokes.replaceAll("-","");
			}
		tokens.add(newtokes);//Adds the combined hyphen token and adds it to list
		for(int i=0;i<tokens.size();i++)
		{//Block that lowercases and stems.
			tokens.set(i,tokens.get(i).toLowerCase());
			stemmer.setCurrent(tokens.get(i));
			stemmer.stem();
			tokens.set(i,stemmer.getCurrent());
		}
		return tokens;
		}

}
