package it.unipd.dei.bd1718;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;
import edu.stanford.nlp.util.CoreMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Collection of functions that allow to transform texts to sequence
 * of lemmas using lemmatization. An alternative process is
 * stemming. For a discussion of the difference between stemming and
 * lemmatization see this link: https://nlp.stanford.edu/IR-book/html/htmledition/stemming-and-lemmatization-1.html
 */
public class Lemmatizer {

  private static StanfordCoreNLP pipeline;

  private static Logger logger = LoggerFactory.getLogger(Lemmatizer.class);

  static {
    // Create StanfordCoreNLP object properties, with POS tagging
    // (required for lemmatization), and lemmatization
    Properties props;
    props = new Properties();
    props.put("annotators", "tokenize,ssplit,pos,lemma");

    // StanfordCoreNLP loads a lot of models, so you probably
    // only want to do this once per execution
    pipeline = new StanfordCoreNLP(props);
  }

  /**
   * Some symbols are interpreted as tokens. This regex allows us to exclude them.
   */
  private static Pattern symbols = Pattern.compile("^[',\\.`/-_]+$");

  /**
   * A set of special tokens that are present in the Wikipedia dataset, and that should be removed
   */
  private static HashSet<String> specialTokens =
          new HashSet<>(Arrays.asList("-lsb-", "-rsb-", "-lrb-", "-rrb-", "'s", "--"));

  public static ArrayList<ArrayList<String>> lemmatizedSentences(String doc) {
    long start = System.currentTimeMillis();
    Annotation document = new Annotation(doc);
    pipeline.annotate(document);

    ArrayList<ArrayList<String>> sentences = new ArrayList<>();

    long numLemmas = 0;
    for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
      ArrayList<String> lemmas = new ArrayList<>();
      for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
        String lemma = token.get(CoreAnnotations.LemmaAnnotation.class);
        numLemmas++;
        // Remove symbols
        if (!symbols.matcher(lemma).matches() && !specialTokens.contains(lemma)) {
          lemmas.add(lemma);
        }
      }
      if (!lemmas.isEmpty()) {
        sentences.add(lemmas);
      }
    }
    long end = System.currentTimeMillis();
    logger.info("Performed initial annotation in " + (end - start) + " ms");
    double throughput = 1000 * ((double) numLemmas) / (end - start);
    logger.info("Annotation throughput " + throughput + " lemmas/s");

    return sentences;
  }

  public static void main(String[] args) {
    System.out.println(lemmatizedSentences(
            "This is a sentence. This is another. The whole thing is a document made of sentences."));
  }

}