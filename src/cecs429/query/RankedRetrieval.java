package cecs429.query;

import cecs429.index.DiskIndexWriter;
import cecs429.index.DiskPositionalIndex;
import cecs429.index.Index;
import cecs429.index.Posting;
import cecs429.text.Normalize;

import java.util.*;

public class RankedRetrieval implements Ranking, RankFormula {
    @Override
    public ArrayList<Accumalator> rankAlgorithm(String query, DiskPositionalIndex index) {
        HashMap<Integer, Double> accMap = new HashMap<>();
        ArrayList<Accumalator> results = new ArrayList<>();
        String[] tokens = query.split(" ");
        double N = index.getDocumentCount();
        Normalize processor = new Normalize("en");
        List<Posting> postList = new ArrayList<>();
        for(int i = 0; i < tokens.length; i++)
        {
            List<String> myList = new ArrayList<>(processor.processToken(tokens[i]));


            //Go back to give option for user to choose
            for (String token: myList) {
                postList = index.getPostings(token, true);
            }
            if(postList == null)
            {
                return null;
            }

            double dft = postList.size();
            double div = N/dft;
            double wqt = Math.log(1 + div);
            double accumulator = 0;


            //loop through postings
            for(int j = 0; j < postList.size(); j++){
                Posting p = postList.get(j);

                //check for existing accumulator in hashmap
                if(accMap.containsKey(p.getDocumentId())){
                    accumulator = accMap.get(p.getDocumentId());
                }
                else
                    accumulator = 0;

                //get tftd = size of positions array list
                double tftd = p.getPositions().size();
                //get wdt
                double wdt = 1 + Math.log(tftd);
                //increment accumulator --> wdt * wqt
                accumulator += wdt * wqt;
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
                double ld = index.getDocWeight(entry.getKey());
                //create new accumulator posting object
                Accumalator acc = new Accumalator(entry.getKey(), entry.getValue()/ld);

                //add posting to pq
                pq.add(acc);
            }
        }

        //loop through first 10 entrues in pq and break if theres is less than 10
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

    @Override
    public double getWqt(Index i, String term) {
        return 0;
    }

    @Override
    public double getWdt(Index i, String term, int docId) {
        return 0;
    }

    @Override
    public double getLd(int docId) {
        return 0;
    }

}
