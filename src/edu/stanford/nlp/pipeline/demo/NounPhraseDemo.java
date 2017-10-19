package edu.stanford.nlp.pipeline.demo;

import java.io.*;
import java.sql.*;
import java.util.*;

import edu.stanford.nlp.io.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.util.*;

public class NounPhraseDemo {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Usage: java edu.stanford.nlp.pipeline.demo.NounPhraseDemo <input.txt> <output.sqlite3>");
            System.exit(1);
        }

        Connection connection = null;
        try {
            Properties props = new Properties();
            props.setProperty("annotators", "tokenize, ssplit, pos, parse");
            StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
            Annotation annotation = new Annotation(IOUtils.slurpFileNoExceptions(args[0]));
            pipeline.annotate(annotation);

            List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
            Map<String, Integer> phrases = new HashMap<String, Integer>();
            for (CoreMap sentence : sentences) {
                for (Tree subtree : sentence.get(TreeCoreAnnotations.TreeAnnotation.class)) {
                    if (subtree.value().equals("NP")) {
                        String phrase = String.join(" ", subtree.getLeaves().stream().map(Tree::toString).toArray(String[]::new));
                        phrases.put(phrase, phrases.getOrDefault(phrase, 0) + 1);
                    }
                }
            }

            connection = DriverManager.getConnection("jdbc:sqlite:" + args[1]);
            PreparedStatement getPhrase = connection.prepareStatement("SELECT id FROM nd_phrase WHERE text = ?");
            PreparedStatement getSentence = connection.prepareStatement("SELECT id FROM nd_sentence WHERE text = ? AND page = ? AND filename = ?");
            PreparedStatement insertPhrase = connection.prepareStatement("INSERT OR IGNORE INTO nd_phrase (text, count) VALUES (?, ?)");
            PreparedStatement insertRelation = connection.prepareStatement("INSERT OR IGNORE INTO nd_phrase_sentences (phrase_id, sentence_id) VALUES (?, ?)");
            PreparedStatement insertSentence = connection.prepareStatement("INSERT OR IGNORE INTO nd_sentence (text, page, filename) VALUES (?, ?, ?)");
            int phraseCount = 0, relationCount = 0, sentenceCount = 0;

            for (String phrase : phrases.keySet()) {
                insertPhrase.setString(1, phrase);
                insertPhrase.setInt(2, phrases.get(phrase));
                phraseCount += insertPhrase.executeUpdate();
            }

            String filename = new File(args[0]).getName();
            for (CoreMap sentence : sentences) {
                insertSentence.setString(1, sentence.toString());
                insertSentence.setInt(2, -1);
                insertSentence.setString(3, filename);
                sentenceCount += insertSentence.executeUpdate();

                getSentence.setString(1, sentence.toString());
                getSentence.setInt(2, -1);
                getSentence.setString(3, filename);
                ResultSet rs = getSentence.executeQuery();
                int sentenceId = rs.getInt(1);

                for (Tree subtree : sentence.get(TreeCoreAnnotations.TreeAnnotation.class)) {
                    if (subtree.value().equals("NP")) {
                        String phrase = String.join(" ", subtree.getLeaves().stream().map(Tree::toString).toArray(String[]::new));
                        getPhrase.setString(1, phrase);
                        rs = getPhrase.executeQuery();
                        int phraseId = rs.getInt(1);

                        insertRelation.setInt(1, phraseId);
                        insertRelation.setInt(2, sentenceId);
                        relationCount += insertRelation.executeUpdate();
                    }
                }
            }
            System.out.format("%d phrases, %d sentences, and %d relations inserted%n", phraseCount, sentenceCount, relationCount);
        }
        catch(SQLException e)
        {
            System.err.println(e.getMessage());
        }
        finally
        {
            try
            {
                if(connection != null)
                    connection.close();
            }
            catch(SQLException e)
            {
                System.err.println(e);
            }
        }
    }
}
