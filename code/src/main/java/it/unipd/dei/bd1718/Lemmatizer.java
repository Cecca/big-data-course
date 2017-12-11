package it.unipd.dei.bd1718;

import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Collection of functions that allow to transform texts to sequence
 * of lemmas using lemmatization. An alternative process is
 * stemming. For a discussion of the difference between stemming and
 * lemmatization see this link: https://nlp.stanford.edu/IR-book/html/htmledition/stemming-and-lemmatization-1.html
 */
public class Lemmatizer {

  /**
   * Some symbols are interpreted as tokens. This regex allows us to exclude them.
   */
  private static Pattern symbols = Pattern.compile("^[',\\.`/-_]+$");

  /**
   * A set of special tokens that are present in the Wikipedia dataset
   */
  private static HashSet<String> specialTokens =
          new HashSet<>(Arrays.asList("-lsb-", "-rsb-", "-lrb-", "-rrb-", "'s", "--"));

  /**
   * Transform a single document in the sequence of its lemmas.
   */
  public static ArrayList<String> lemmatize(String doc) {
    Document d = new Document(doc.toLowerCase());
    // Count spaces to allocate the vector to the right size and avoid trashing memory
    int numSpaces = 0;
    for (int i = 0; i < doc.length(); i++) {
      if (doc.charAt(i) == ' ') {
        numSpaces++;
      }
    }
    ArrayList<String> lemmas = new ArrayList<>(numSpaces);

    for (Sentence sentence : d.sentences()) {
      for (String lemma : sentence.lemmas()) {
        // Remove symbols
        if (!symbols.matcher(lemma).matches() && !specialTokens.contains(lemma)) {
          lemmas.add(lemma);
        }
      }
    }

    return lemmas;
  }

  public static WikiPage lemmatize(WikiPage wp) {
    ArrayList<String> lemmas = lemmatize(wp.getText());
    StringBuilder newText = new StringBuilder();
    for(String lemma : lemmas) {
      newText.append(lemma).append(' ');
    }
    wp.setText(newText.toString());
    return wp;
  }

  public static ArrayList<ArrayList<String>> lemmatizedSentences(String doc) {
    Document d = new Document(doc.toLowerCase());

    ArrayList<ArrayList<String>> sentences = new ArrayList<>();

    for (Sentence sentence : d.sentences()) {
      ArrayList<String> lemmas = new ArrayList<>();
      for (String lemma : sentence.lemmas()) {
        // Remove symbols
        if (!symbols.matcher(lemma).matches() && !specialTokens.contains(lemma)) {
          lemmas.add(lemma);
        }
      }
      if (!lemmas.isEmpty()) {
        sentences.add(lemmas);
      }
    }

    return sentences;
  }

  public static void foreachLemma(String doc, Consumer<String> fn) {
    Document d = new Document(doc.toLowerCase());

    for (Sentence sentence : d.sentences()) {
      for (String lemma : sentence.lemmas()) {
        // Remove symbols
        if (!symbols.matcher(lemma).matches() && !specialTokens.contains(lemma)) {
          fn.accept(lemma);
        }
      }
    }
  }

  public static void main(String[] args) {
    System.out.println(lemmatizedSentences(
            "This is a sentence. This is another. The whole thing is a document made of sentences."));
  }

}