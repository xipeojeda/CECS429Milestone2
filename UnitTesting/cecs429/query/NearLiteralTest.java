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

class NearLiteralTest {
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
    @DisplayName("Test for [smash /1 super]")
    @Test
    void nearQuery() {
        List<String> actualResults = Arrays.asList("test2.txt");

        String query = "[smash /1 super]";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for [super /1 smash]")
    @Test
    void nearQuery2() {
        List<String> actualResults = Arrays.asList("test2.txt");

        String query = "[super /1 smash]";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for [super /2 super]")
    @Test
    void nearQuery4() {
        List<String> actualResults = Arrays.asList("test2.txt");

        String query = "[super /2 super]";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for [terrell /1 neal]")
    @Test
    void nearQuery5() {
        List<String> actualResults = Arrays.asList("test4.txt");

        String query = "[terrell /1 neal]";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for [what /3 super-cool]")
    @Test
    void nearQuery6() {
        List<String> actualResults = Arrays.asList("test5.txt");

        String query = "[what /3 super-cool]";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for [what /1 supercool]")
    @Test
    void nearQuery7() {
        List<String> actualResults = Arrays.asList("test5.txt");

        String query = "[what /1 supercool]";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for [hello /10 world]")
    @Test
    void nearQuery8() {
        List<String> actualResults = Arrays.asList("test1.txt");

        String query = "[hello /10 world]";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for [world /10 hello]")
    @Test
    void nearQuery9() {
        List<String> actualResults = Arrays.asList();

        String query = "[world /10 hello]";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }

    @DisplayName("Test for [dummy /100 test]")
    @Test
    void nearQuery10() {
        List<String> actualResults = Arrays.asList();

        String query = "[dummy /100 test]";
        List<String> queryResults = new ArrayList<>();
        for (Posting p : booleanQueryParser.parseQuery(query).getPostings(index, normalize)) {
            queryResults.add(corpus.getDocument(p.getDocumentId()).getTitle());
        }

        assertIterableEquals(actualResults, queryResults);
    }
}