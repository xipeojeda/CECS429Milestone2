package cecs429.query;

import cecs429.documents.DirectoryCorpus;
import cecs429.documents.Document;
import cecs429.documents.DocumentCorpus;
import cecs429.index.Index;
import cecs429.index.PositionalInvertedIndex;
import cecs429.index.Posting;
import cecs429.text.Normalize;
import cecs429.text.EnglishTokenStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for OrQuery.java
 */
class OrQueryTest {
    /*
        Block for creating test corpus, index and BooleanQueryParser
     */
    private final DocumentCorpus corpus = DirectoryCorpus.loadTextDirectory(Paths.get("C:\\JUnitTestFiles").toAbsolutePath(), ".txt");
    private final Index index = indexCorpus(corpus);

    private final BooleanQueryParser booleanQueryParser = new BooleanQueryParser();
    private final Normalize normalize = new Normalize();

    private static PositionalInvertedIndex indexCorpus(DocumentCorpus corpus) {
        PositionalInvertedIndex pInvIdx = new PositionalInvertedIndex(); //Inverted index
        Iterable<Document> documentsIterable = corpus.getDocuments(); //Make documents iterable
        Normalize normalize = new Normalize(); //WORD STEMMING
        HashSet<String> vocabulary = new HashSet<>();

        for (Document doc : documentsIterable) {
            EnglishTokenStream ets = new EnglishTokenStream(doc.getContent());

            int count = 0; //Keep track of position
            for (String str : ets.getTokens()) {
                List<String> terms = normalize.processToken(str);

                for (String term: terms) {
                    vocabulary.add(term);
                    pInvIdx.addTerm(term, doc.getId(), count);
                    count++;
                }
            }
            try {
                ets.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        return pInvIdx;
    }

    /*
        Block for test cases
     */
    @DisplayName("Test for super-cool + \"neal terrell\"")
    @Test
    void orQuery() {
        List<String> actualResults = Arrays.asList("test3.txt", "test4.txt", "test5.txt");

        String query = "super-cool + \"neal terrell\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for ?WHAT? + \"ur mum\"")
    @Test
    void orQuery2() {
        List<String> actualResults = Arrays.asList("test5.txt");

        String query = "?WHAT? + \"ur mum\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for \"world! WHAT\" + terrell")
    @Test
    void orQuery3() {
        List<String> actualResults = Arrays.asList("test3.txt", "test4.txt");

        String query = "\"world! WHAT\" + terrell";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for world!! + what!!")
    @Test
    void orQuery4() {
        List<String> actualResults = Arrays.asList("test1.txt", "test5.txt");

        String query = "world!! + what!!";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for world + what + super")
    @Test
    void orQuery5() {
        List<String> actualResults = Arrays.asList("test1.txt", "test2.txt", "test5.txt");

        String query = "world + what + super";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for shakes + \"jamba juice\"")
    @Test
    void orQuery6() {
        List<String> actualResults = Arrays.asList();

        String query = "shakes + \"jamba juice\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for cool + super smash")
    @Test
    void orQuery7() {
        List<String> actualResults = Arrays.asList("test2.txt", "test5.txt");

        String query = "cool + super smash";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for cool + \"super smash\"")
    @Test
    void orQuery8() {
        List<String> actualResults = Arrays.asList("test2.txt", "test5.txt");

        String query = "cool + \"super smash\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for \"??WhAT?? ?supercool?\" + \"hElLO? ;world;\"")
    @Test
    void orQuery9() {
        List<String> actualResults = Arrays.asList("test1.txt", "test5.txt");

        String query = "\"??WhAT?? ?supercool?\" + \"hElLO? ;world;\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for ??WhAT?? ?supercool? + hElLO? ;world;")
    @Test
    void orQuery10() {
        List<String> actualResults = Arrays.asList("test1.txt", "test5.txt");

        String query = "??WhAT?? ?supercool? + hElLO? ;world;";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("\"neal what\" + super")
    @Test
    void orQuery11() {
        List<String> actualResults = Arrays.asList("test2.txt", "test5.txt");

        String query = "\"neal what\" + super";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }
}