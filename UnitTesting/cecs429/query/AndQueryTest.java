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
 * Test class for AndQuery.java
 */
class AndQueryTest {
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
    @DisplayName("Test for neal terrell")
    @Test
    void andQuery() {
        List<String> actualResults = Arrays.asList("test3.txt", "test4.txt");

        String query = "neal terrell";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for hello super neal")
    @Test
    void andQuery2() {
        List<String> actualResults = Arrays.asList();

        String query = "hello super neal";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for hello?? hello! hello")
    @Test
    void andQuery3() {
        List<String> actualResults = Arrays.asList("test1.txt");

        String query = "hello?? hello! hello";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for ?world! super;")
    @Test
    void andQuery4() {
        List<String> actualResults = Arrays.asList();

        String query = "?world! super;";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for !!!!!!!!!!!!!terrell??????????")
    @Test
    void andQuery5() {
        List<String> actualResults = Arrays.asList("test3.txt", "test4.txt");

        String query = "!!!!!!!!!!!!!terrell??????????";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for \"terrible tyrant\"")
    @Test
    void andQuery6() {
        List<String> actualResults = Arrays.asList();

        String query = "\"terrible tyrant\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for \"world what\"")
    @Test
    void andQuery7() {
        List<String> actualResults = Arrays.asList();

        String query = "\"world what\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for world what")
    @Test
    void andQuery8() {
        List<String> actualResults = Arrays.asList();

        String query = "world what";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for super-cool")
    @Test
    void andQuery9() {
        List<String> actualResults = Arrays.asList("test2.txt", "test5.txt", "test5.txt", "test5.txt"); /////hmmmmmmmm

        String query = "super-cool";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for hello world!")
    @Test
    void andQuery10() {
        List<String> actualResults = Arrays.asList("test1.txt");

        String query = "hello world!";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }
}