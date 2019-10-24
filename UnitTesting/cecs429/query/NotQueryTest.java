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

class NotQueryTest {
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
    @DisplayName("Test for ??super! -neal")
    @Test
    void notQuery() {
        List<String> actualResults = Arrays.asList("test2.txt");

        String query = "??super! -neal";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for neal -super?")
    @Test
    void notQuery2() {
        List<String> actualResults = Arrays.asList("test3.txt", "test4.txt");

        String query = "neal -super";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for neal -TERRELL??")
    @Test
    void notQuery3() {
        List<String> actualResults = Arrays.asList();

        String query = "neal -TERRELL??";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for super-cool -smash")
    @Test
    void notQuery4() {
        List<String> actualResults = Arrays.asList("test5.txt");

        String query = "super-cool -smash";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for hello!!!!!!!! -;;;;super;;;;")
    @Test
    void notQuery5() {
        List<String> actualResults = Arrays.asList("test1.txt");

        String query = "hello!!!!!!!! -;;;;super;;;;";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for super-cool -super") //Brain teaser
    @Test
    void notQuery6() {
        List<String> actualResults = Arrays.asList();

        String query = "super-cool -super";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for ??terrell?? -hello!")
    @Test
    void notQuery7() {
        List<String> actualResults = Arrays.asList("test3.txt", "test4.txt");

        String query = "??terrell?? -hello!";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for -hello world!")
    @Test
    void notQuery8() {
        List<String> actualResults = Arrays.asList();

        String query = "-hello world!";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for -hello")
    @Test
    void notQuery9() {
        List<String> actualResults = Arrays.asList("test2.txt", "test3.txt", "test4.txt", "test5.txt");

        String query = "-hello";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for *what -smash")
    @Test
    void notQuery10() {
        List<String> actualResults = Arrays.asList("test5.txt");

        String query = "\"*what super-cool\" -smash";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }
}