package cecs429.index;

import cecs429.documents.DirectoryCorpus;
import cecs429.documents.Document;
import cecs429.documents.DocumentCorpus;
import cecs429.text.Normalize;
import cecs429.text.EnglishTokenStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for PositionalInvertedIndex.java
 */
class PositionalInvertedIndexTest {
    /*
        Block for creating test corpus and index
     */
    private final DocumentCorpus corpus = DirectoryCorpus.loadTextDirectory(Paths.get("C:\\JUnitTestFiles").toAbsolutePath(), ".txt");
    private final PositionalInvertedIndex index = indexCorpus(corpus);

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
    @DisplayName("Test for term \"neal\"")
    @Test
    void testGetPostings() {
        List<String> realResult = new ArrayList<>(Arrays.asList("test3.txt", "test4.txt"));
        List<String> whatIndexReturns =  new ArrayList<>();
        for (Posting p : index.getPostings("neal")) {
            whatIndexReturns.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }
        assertIterableEquals(realResult, whatIndexReturns);
    }

    @DisplayName("Test for term \"terrell\"")
    @Test
    void testGetPostings2() {
        List<String> realResult = new ArrayList<>(Arrays.asList("test3.txt", "test4.txt"));
        List<String> whatIndexReturns =  new ArrayList<>();
        for (Posting p : index.getPostings("terrel")) {
            whatIndexReturns.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }
        assertIterableEquals(realResult, whatIndexReturns);
    }

    @DisplayName("Test for term \"what\"")
    @Test
    void testGetPostings3() {
        List<String> realResult = new ArrayList<>(Arrays.asList("test5.txt"));
        List<String> whatIndexReturns =  new ArrayList<>();
        for (Posting p : index.getPostings("what")) {
            whatIndexReturns.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }
        assertIterableEquals(realResult, whatIndexReturns);
    }

    @DisplayName("Test for term \"supercool\"")
    @Test
    void testGetPostings4() {
        List<String> realResult = new ArrayList<>(Arrays.asList("test5.txt"));
        List<String> whatIndexReturns =  new ArrayList<>();
        for (Posting p : index.getPostings("supercool")) {
            whatIndexReturns.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }
        assertIterableEquals(realResult, whatIndexReturns);
    }

    @DisplayName("Test for term \"super-cool\"")
    @Test
    void testGetPostings5() {
        List<String> realResult = new ArrayList<>(Arrays.asList());
        List<String> whatIndexReturns =  new ArrayList<>();
        for (Posting p : index.getPostings("super-cool")) {
            whatIndexReturns.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }
        assertIterableEquals(realResult, whatIndexReturns);
    }

    @DisplayName("Test for term \"super\"")
    @Test
    void testGetPostings6() {
        List<String> realResult = new ArrayList<>(Arrays.asList("test2.txt", "test5.txt"));
        List<String> whatIndexReturns =  new ArrayList<>();
        for (Posting p : index.getPostings("super")) {
            whatIndexReturns.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }
        assertIterableEquals(realResult, whatIndexReturns);
    }

    @DisplayName("Test for term \"world!\"")
    @Test
    void testGetPostings7() {
        List<String> realResult = new ArrayList<>(Arrays.asList());
        List<String> whatIndexReturns =  new ArrayList<>();
        for (Posting p : index.getPostings("world!")) {
            whatIndexReturns.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }
        assertIterableEquals(realResult, whatIndexReturns);
    }

    @DisplayName("Test for term \"world\"")
    @Test
    void testGetPostings8() {
        List<String> realResult = new ArrayList<>(Arrays.asList("test1.txt"));
        List<String> whatIndexReturns =  new ArrayList<>();
        for (Posting p : index.getPostings("world")) {
            whatIndexReturns.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }
        assertIterableEquals(realResult, whatIndexReturns);
    }

    @DisplayName("Test vocabulary")
    @Test
    void testVocabulary() {
        List<String> realResult = new ArrayList<>(Arrays.asList("cool", "hello", "neal", "smash", "super", "supercool", "terrel", "what", "world"));
        assertIterableEquals(realResult, index.getVocabulary());
    }

    @DisplayName("Test addTerm")
    @Test
    void testAddTerm() {
        PositionalInvertedIndex test = new PositionalInvertedIndex();
        index.addTerm("poop", 3,3);
    }
}