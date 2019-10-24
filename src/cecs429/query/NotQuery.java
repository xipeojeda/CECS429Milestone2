package cecs429.query;

import cecs429.index.Index;
import cecs429.index.Posting;
import cecs429.text.Normalize;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class NotQuery implements QueryComponent {
    private List<QueryComponent>  mComponents;
    public NotQuery(List<QueryComponent>components) {
        mComponents = components;
    }

    @Override
    public List<Posting> getPostings(Index index,Normalize normalize) {
        List<Posting> result = new ArrayList<>();
        List<Posting> temp = new ArrayList<>();
        List<Posting> temp2 = new ArrayList<>();
        if (mComponents.size() == 0)  //check if mComponents list is empty
            return temp2;
        else
            temp2.addAll(mComponents.get(0).getPostings(index,normalize)); //Get postings of first index of "mComponents"
        //gather posting of QueryComponents and intersect with results list postings
        for (int i = 1; i < mComponents.size(); i++) {
            temp = mComponents.get(i).getPostings(index,normalize); //"AND NOT" algorithm happens here after getting the second component to be notted.
            result = notIntersection(temp2,temp); //CALL HELPER METHOD
        }

        return result;
    }

    public List<Posting> notIntersection(List<Posting> p1, List<Posting> p2){
        List<Posting> answer = new ArrayList<>();

        int i = 0, j = 0;
        Posting post1, post2;
        while(i < p1.size() && j < p2.size()){
            post1 = p1.get(i);
            post2 = p2.get(j);
            // If the document ids are the same below, we do not add anything and we increment both pointers.
            if(post1.getDocumentId() == post2.getDocumentId()) {
                i++;
                j++;
            }
            //Only when document id of posting 1 is less than 2 do we add postings from doc 1
            else if (post1.getDocumentId() < post2.getDocumentId())
            {
                answer.add(post1);
                i++;

            }
            else {
                j++;
            }
        }

        return answer;
    }

    @Override
    public String toString() {
        return
                String.join(" ", mComponents.stream().map(c -> c.toString()).collect(Collectors.toList()));
    }
}
