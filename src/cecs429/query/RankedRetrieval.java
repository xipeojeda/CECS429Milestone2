package cecs429.query;

import cecs429.index.DiskIndexWriter;
import cecs429.index.DiskPositionalIndex;
import cecs429.index.Index;
import cecs429.index.Posting;
import cecs429.text.Normalize;

import java.util.*;

public class RankedRetrieval implements Ranking {
    private String lang;
    
    @Override
    public ArrayList<Accumalator> rankAlgorithm(String query, DiskPositionalIndex index,String lang) {
    	this.lang=lang;
        HashMap<Integer, Double> accMap = new HashMap<>();
        ArrayList<Accumalator> results = new ArrayList<>();
        String[] tokens = query.split(" ");
        double N = index.getDocumentCount();
        Normalize processor = new Normalize(lang); //Normalizes in English, fix for language
        List<Posting> postList = new ArrayList<>();
        double tftd = 0;
        double wdt = 0;
       
        for(int i = 0; i < tokens.length; i++)
        {
            List<String> myList = new ArrayList<>(processor.processToken(tokens[i])); //Normalized tokens from query

            //Go back to give option for user to choose
            for (String token: myList)
                postList = index.getPostings(token, true);

            if(postList == null)
                return null;

            double dft = postList.size(); //Doc frequency given term
            double div = N/dft; //Used to calculate Wq,t
            double wqt = Math.log(1 + div); //Weigh of query given term
            double accumulator = 0; //Keep track of score for each document

            //loop through postings
            for (Posting p : postList) {

                //check for existing accumulator in hashmap
                if(accMap.containsKey(p.getDocumentId())) //1) Acquire an accumulator val Ad
                    accumulator = accMap.get(p.getDocumentId());
                else
                    accumulator = 0;

                //get tftd = size of positions array list
                tftd = p.getPositions().size();
                //get wdt
                wdt = 1 + Math.log(tftd); //2) Calculate Wd,t
                //increment accumulator --> wdt * wqt
                accumulator += (wdt * wqt); //3) Increase Ad by Wd,t x Wq,t
                //add to map
                accMap.put(p.getDocumentId(), accumulator);
            }
        }
        //create a pq with the size of the accumulator map
        //use comparator in AccumulatorSort
        PriorityQueue<Accumalator> pq = new PriorityQueue<>(accMap.size(), new AccumulatorSort());
        
        //loop through accMap
        for(Map.Entry<Integer, Double> entry : accMap.entrySet()){
            if(entry.getValue() > 0){
                //need to add method in diskpositionalindex
            	
                double ld = index.getDocWeight(entry.getKey()); //BIG PROBLEMO
                //create new accumulator posting object
                //For each non-zero Ad, divide Ad by Ld where Ld is read from the docWeights.bin file
                Accumalator acc = new Accumalator(entry.getKey(), (double)entry.getValue()/ld);
                //add posting to pq
                pq.add(acc);
            }
        }

        //loop through first 10 entries in pq and break if there's less than 10
        int i = 0;
        while(i < 10){
            if(pq.peek() != null){
                results.add(pq.remove());
            }
            else
                break;
            i++;
        }
        return results;
    }
}
