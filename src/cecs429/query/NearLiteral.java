package cecs429.query;

import cecs429.index.Index;
import cecs429.index.Posting;
import cecs429.text.Normalize;

import java.util.ArrayList;
import java.util.List;

public class NearLiteral implements QueryComponent {

    private List<QueryComponent> mComponents;
    // created list where to store terms
    public NearLiteral (List<QueryComponent> components) {
        mComponents = components;
    }
    private List<String> mTerms = new ArrayList<>();
    //used to hold the distance we want our near query to be
    private int x;

    //constructor where we pass the tokens and distance
    public NearLiteral(String token1, String token2, int distance)
    {
        //adding the tokens to list
        mTerms.add(token1);
        mTerms.add(token2);
        x = distance;
    }
    //overide get postings
    @Override
    public List<Posting> getPostings(Index index, Normalize normalize) {
        //creating result list
        List<Posting> result = new ArrayList<>();
        //temporary posting temp list
        List<Posting> temp = new ArrayList<>();
        //list to hold normalized terms
        List<String> processedList = new ArrayList<>();
        //traverse through list of terms and normalize
        for(String s: mTerms) {
            processedList.addAll(normalize.processToken(s));
        }
        //if no terms return empty list
        if(mTerms.size() == 0)
            return result;
        else
            //add all postings of index 1
            result.addAll(index.getPostings(processedList.get(0)));
        // traverse through the amount of items in mTerms list
        for(int i = 0; i < mTerms.size();i++) {
            temp = index.getPostings(processedList.get(i)); //adding the postings of the normalized terms
            result=positionalMerge(result,temp, x); //do the positional merge
        }
        return result;
    }


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
                    if (pp2.get(n) - pp1.get(m) <= position) {
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
