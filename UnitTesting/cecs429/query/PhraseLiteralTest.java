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

class PhraseLiteralTest {
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
    @DisplayName("Test for \"neal terrell\"")
    @Test
    void phraseQuery() {
        List<String> actualResults = Arrays.asList("test3.txt");

        String query = "\"neal terrell\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for \"TERRELL! NEAL!\"")
    @Test
    void phraseQuery2() {
        List<String> actualResults = Arrays.asList("test4.txt");

        String query = "\"TERRELL! NEAL!\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for \"??hELlo! ;world;\"")
    @Test
    void phraseQuery3() {
        List<String> actualResults = Arrays.asList("test1.txt");

        String query = "\"??hELlo! ;world;\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for \"super smash\"")
    @Test
    void phraseQuery4() {
        List<String> actualResults = Arrays.asList("test2.txt");

        String query = "\"super smash\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for \"super smash super smash\"")
    @Test
    void phraseQuery5() {
        List<String> actualResults = Arrays.asList();

        String query = "\"super smash super smash\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for \"what?? super-cool\"")
    @Test
    void phraseQuery6() {
        List<String> actualResults = Arrays.asList("test5.txt");

        String query = "\"what?? super-cool\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for \"what supercool\"")
    @Test
    void phraseQuery7() {
        List<String> actualResults = Arrays.asList("test5.txt");

        String query = "\"what supercool\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for \"Hello, Neal!\"")
    @Test
    void phraseQuery8() {
        List<String> actualResults = Arrays.asList();

        String query = "\"Hello, Neal!\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for \"world? hello?\"")
    @Test
    void phraseQuery9() {
        List<String> actualResults = Arrays.asList();

        String query = "\"world? hello?\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for \"super-cool\"")
    @Test
    void phraseQuery10() {
        List<String> actualResults = Arrays.asList("test2.txt", "test5.txt"); //Tokenization split on hyphens

        String query = "\"super-cool\"";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }
}