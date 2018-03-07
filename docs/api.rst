.. _spark-api:

Most useful Spark methods
=========================

.. highlight:: Java

This page presents detailed API, with examples, of some of the most useful Spark functions. Note that the ultimate source of information on this topic is the `official Java API of Spark <https://spark.apache.org/docs/2.2.0/api/java/>`_.

.. topic:: On functions and interfaces

  Before describing the most useful Spark APIs, we need to spend a word on Java's anonymous functions and Java interfaces.

  Starting from Java 8, we can pass functions as arguments to methods.
  Therefore, there is the need to provide a way to method authors to declare that their method requires a certain parameter to be a function.
  Rather than introducing a new type, Java solves this problem by defining *functional interfaces*.
  A functional interface is an interface with a single *abstract* method (starting from Java 8, interfaces can have methods with a default implementation, just to complicate things further).
  The Java compiler makes functions with the right parameters and return types implement the functional interface required by a given method call.

  The following is an example of functional interface::

    public interface DistanceFunction<T> {

      public double call(T a, T b);

    }

  The above functional interface can be used to declare a method that accepts as a parameter a distance function, like the following::

    public static <T> ArrayList<T> kCenter(ArrayList<T> points, 
                                           int k,
                                           DistanceFunction<T> distance) {
      // ...
      // use distance.call(points.get(i), points.get(j))
      // to get the distance between the i-th and the j-th points.
    }

  The above method, in turn, can be used as follows::

    ArrayList<Double> pointsOnTheLine;
    kCenter(pointsOnTheLine, 10, (x, y) -> Math.abs(a - b));

  The compiler will take care of checking that the anonymous function we are passing to method ``kCenter`` can implement the functional interface ``DistanceFunction``.
  Note that we did nothing special to make ``DistanceFunction`` a functional interface, other than declaring a *single* abstract method in it.

  Many interfaces of Java's standard library comply with the requirements of functional interfaces. Among the others, some notable examples are ``java.util.Comparator``, ..., plus the newly introduced ``java.function.Function`` and ``java.function.BiFunction`.
  Spark introduces some functional interfaces of its own: ``org.apache.spark.Function``, ``org.apache.spark.FlatMapFunction``, ``org.apache.spark.PairFunction``, ``org.apache.spark.PairFlatMapFunction``.
  The bottom line is that whenever you find a method that requires passing a functional interface, you can pass an anonymous function.

  There are some caveats when using functions and functional interfaces with Spark's Java API.
  Depending on where you run your code (eg. on a cluster), Spark may need to serialize your functions to ship them to executors.
  Making a Java object serializable is as simple as making it inherit from ``java.util.Serializable``, which doesn't even require to implement any method.
  Anonymous functions are potentially serializable, usually.
  An anonymous function is not serializable if it captures a non-serializable object in its body.
  However, even if a function is potentially serializable (because all its captures are serializable), at compile-time it gets "dressed" with the functional interface required by the method to which we are passing it.
  This means that, after compilation, our function will be, for all intents and purposes, a ``java.util.Comparator`` or a ``org.apache.spark.FlatMapFunction``, depending on the context.
  The problem is that some functional interface, like ``java.util.Comparator``, do not inherit from ``Serializable``, therefore our function isn't seen as serializable.
  The consequence is that at runtime our program may crash with a ``TaskNotSerializableException``.
  This issue is due to the Java compiler not being smart enough in this context.
  A workaround is to explicitly implement a class implementing both ``Serializable`` and ``Comparator``, as shown in the example for the ``min`` and ``max`` methods (cfr. :ref:`min and max methods <min-max-methods>`).



.. class:: scala.Tuple2<K, V>::

  In MapReduce, we work with key-value pairs.
  Spark uses this type to represent such pairs.
  You can access the key and the value by means of the methods ``_1()`` and ``_2()``, respectively.
  Here are some examples::

    // Create a new pair
    Tuple2<String, Long> pair = new Tuple2<>("ciao", 1);

    // Access the elements
    pair._1() // = "ciao" 
    pair._2() // = 1


.. class:: org.apache.spark.api.java.JavaRDD<T>

  This class is a local handle for a Resilient Distributed Dataset containing elements of type ``T``.
  It provides functions to transform data and to collect information locally.

  .. rubric:: Transformations

  The following functions are called *transformations*, since they transform a RDD into another one.

  .. function:: map(Function<T, R> func)

    Returns a ``JavaRDD<R>`` by applying the given function to each element of ``this`` independently.
    This method takes as parameter a function accepting a single parameter of type ``T`` and returning another object of type ``R``.
    The following example shows how to transform a RDD of integers into a RDD of doubles by halving each element of the original collection::

      JavaRDD<Integer> numbers;
      JavaRDD<Double> halves = numbers.map((x) -> x / 2.0);

  .. function:: flatMap(FlatMapFunction<T, R> func)
    
    Like ``map``, but the function passed as a parameter can return multiple values.
    This is useful to "explode" a dataset. For example, imagin you have a RDD of sentences and you want to transform it to a RDD of single words::

      JavaRDD<String> sentences;
      JavaRDD<String> words = sentences.flatMap((s) -> {
        return s.split(" ").iterator();
      });

    You can also make the function return a single element, or even no element, using the facilities of ``java.util.Collections``::

      JavaRDD<Integer> numbers;
      JavaRDD<Integer> evenNumbers = numbers.flatMap((x) -> {
        if (x % 2 == 0) {
          // if the number is even, wrap it in a singleton 
          // set and return an iterator of this set.
          return Collections.singleton(x).iterator();
        } else {
          // otherwise just return an empty iterator.
          return Collections.emptyIterator();
        }
      });

  .. function:: filter(Function<T, Boolean> predicate)

    Returns a ``JavaRDD<T>`` containing only the elements of ``this`` for which the given predicate returns true.
    The argument to this method is a function that takes a single parameter of type ``T`` and returns a boolean value.
    The following shows how to obtain a RDD of even numbers from an RDD of integers::

      JavaRDD<Integer> numbers;
      JavaRDD<Double> evenNumbers = numbers.filter((x) -> x % 2 == 0);

    Note that the example above is equivalent to the second example given for ``flatMap``, but much more concise.

  .. function:: sample(boolean withReplacement, double fraction)

    This function is useful to get a unformly distributed sample of the data.
    Returns a RDD of type ``T``.

    * ``withReplacement``: whether or not to consider again already sampled items.
    * ``fraction``: the fraction of elements to sample.

    Here is how to sample 1000 elements out of a rdd, without replacement::

      long size = rdd.count();
      double fraction = 1000.0 / size;
      JavaRdd<T> sampled = rdd.sample(false, fraction);

  .. function:: mapToPair(PairFunction<T,K,V> func)

    Like ``flatMap``, but returns a ``JavaPairRDD<K, V>``.
    This method takes a function as a parameter. This function should accept a single value of type ``T`` and return an iterator of key-value pairs ``Tuple2<K, V>``.
    For example, to map a RDD of sentences to a RDD of words together with inizialized word counts::

      JavaRDD<String> words;
      JavaPairRDD<String, Long> counts = words.mapToPair((x) -> {
        return new Tuple2<>(x, 1);
      }); 

  .. function:: flatMapToPair(PairFlatMapFunction<T,K,V> func)

    Like ``map``, but returns a ``JavaPairRDD<K, V>``.
    This method takes a function as a parameter. This function should accept a single value of type ``T`` and returns and iterator of key-value pairs ``Tuple2<K, V>``.
    For example, to map a RDD of words to a RDD of words together with inizialized word counts::

      JavaRDD<String> sentences;
      JavaPairRDD<String, Long> counts = sentences.flatMapToPair((s) -> {
        String[] words = s.split(" ");
        ArrayList<Tuple2<String, Long>> counts = new ArrayList<>();
        for (String w : words) {
          counts.append(new Tuple2<>(w, 1));
        }
        return counts.iterator();
      });


  .. function:: keyBy(Function<T, K> func)

    Returns a ``JavaPairRDD`` of key-value pairs where the keys are computed from the elements of ``this``.
    This method takes as argument a function that computes a key from a single elements.
    For example, imagine we have a dataset of words, and we want to use as key the length of the words::

      JavaRDD<String> words;
      JavaPairRDD<Int, String> withKeys = words.keyBy((w) -> w.length);

    So if ``words`` is::

      "dog"
      "cat"
      "computer"

    the RDD resulting from the above application of ``keyBy`` will be::

      (3, "dog")
      (3, "cat")
      (8, "computer")

  .. function:: groupBy(Function<T, K> func)

    Returns a ``JavaPairRDD`` with keys provided by ``func`` (as for the method ``keyBy``) where the value associated to each key is an iterable of all the values sharing the same key.
    Consider again the example we did for ``keyBy``, and suppose we want to group words by length::

      JavaRDD<String> words;
      JavaPairRDD<Int, Iterable<String>> grouped = words.groupBy((w) -> w.length);

    if ``this`` is a RDD of strings like the following::

      "dog"
      "cat"
      "computer"

    then the result will be::

      (3, ["cat", "dog"])
      (8, ["computer"])

    where the notation ``[...]`` denotes a list of elements.

  .. rubric:: Actions

  .. function:: count()

    Returns the size of ``this``, that is, the number of elements it contains.

  .. function:: collect()

    Brings all the data contained in this RDD (which may be distributed across multiple executors) to the driver.

    .. warning:: This action needs enough memory on the driver to store **all** the dataset. Use it only on small RDDs. If the dataset is too big, then an ``OutOfMemoryError`` will be thrown and the program will crash.

    For example::

      JavaRDD<String> distributedWords;
      List<String> localWords = distributedWords.collect();
      // localWords is a local copy of all the elements of the distributedWords RDD.

  .. _min-max-methods:

  .. function:: max(java.util.Comparator<T> comp) 
  .. function:: min(java.util.Comparator<T> comp) 

    Return the maximum and minimum element of the RDD, respectively.
    The ordering among elements is given by the ``Comparator`` taken as argument.
    Comparator is an interface defining a single method which must adhere to the following contract.
    Two arguments of the same type are given. If they are equals, the methods should return 0, if the first is smaller a negative number should be returned, if the first is greater then a positive number should be returned.

    These two methods have an issue in Spark's Java API. As we have seen, anonymous functions automatically implement so-called functional interfaces. The interfaces ``java.util.Comparator`` is a functional interface, so we can pass an anonymous function to ``max`` and ``min`` and the Java compiler will happily succeed in the compilation.
    However, Spark needs functions to be serializable, because it has to send them to executors.
    Now, ``java.util.Comparator`` is not ``Serializable``, so anonymous functions passed to ``max`` and ``min`` are not serializable, even if they could be. This is a limitation of the Java compiler.
    Therefore, passing an anonymous function to ``max`` or ``min`` will cause your code to crash at runtime with a ``TaskNotSerializableException``.

    There is a workaround, that is to explicitly define a class implenting both ``Comparator`` and ``Serializable``.
    The following example finds the longest word in a RDD of words::

      // In its own file
      public class LengthComparator implements Serializable, Comparator<String> {

        public int compare(String a, String b) {
          if (a.length < b.length) return -1;
          else if (a.length > b.length) return 1;
          return 0;
        }

      }

      // Then, in the body of some other method
      JavaRDD<String> words;
      String longest = words.max(new LengthComparator());

    If you want, you can define ``LengthComparator`` as an inner class of another class.
    The important thing is to make it a ``static`` inner class.

  .. function:: reduce(Function2<T, T, T> func)

    Get a single value of type ``T`` by combining all the values of the RDD according to the function ``func``.
    The function **must** be associative and commutative, since there is no guarantee on the order of application to the elements of the RDD.
    For example, to get the sum of all the elements of a RDD of integers::

      JavaRDD<Integer> numbers;
      int sum = numbers.reduce((x, y) -> x + y);


.. class:: org.apache.spark.api.java.JavaPairRDD<K, V>

  This class, similarly to its sibiling ``JavaRDD`` is a local handle to a distributed collection, but this time we are dealing with a collection of key-value pairs.

  You can go back and forth from ``JavaRDD<Tuple2<K, V>>`` to ``JavaPairRDD<K, V>``. To convert a ``JavaRDD`` of `'Tuple2`` objects to a ``JavaPairRDD``, you can use the static methoc ``JavaPairRDD.fromRDD``, as in this example::

    JavaRDD<Tuple2<String, Integer>> wordCounts;
    JavaPairRDD<String, Integer> wordCountsPairRDD = JavaPairRDD.fromRDD(wordCounts);

  Note that the two RDD represent the same data, but ``JavaPairRDD`` gives you access to more methods, in particular the ones to deal with key-value pairs.
  If you need to convert back to a ``JavaRDD`` from a ``JavaPairRDD``, you can use the ``toRDD`` method of ``JavaPairRDD``, as follows::

    JavaPairRDD<String, Integer> wordCountsPairRDD;
    JavaRDD<Tuple2<String, Integer>> wordCounts = wordCountsPairRDD.toRDD();

  Just like the class ``JavaRDD``, we can broadly classify methods into transformations and actions.
  Many methods of ``JavaRDD`` are still accessible, and some new ones are available.
  In particular, methods with **Values** in their name work like their ``JavaRDD`` counterparts, modifying only the values and leaving keys untouched.

  .. rubric:: Transformations

  .. function:: mapValues(Function<V, R> func)
  .. function:: flatMapValues(FlatMapFunction<V, R> func)

    Apply the given function to the **values** of this RDD, leaving keys unchanged. ``mapValues`` accepts a function that takes a single element and returns a single element, while ``flatMapValues`` accepts a function that from a single element returns an iterable of zero or more results.

  .. function:: groupByKey()

    Groups together all the values with the same key. The result of this transformation is a ``JavaPairRDD`` with the same set of keys, and with all the values associated to the same key collected in an ``Iterable``.::

      // An RDD of strings, keyed by their lengths. For instance
      // (3, "dog")
      // (3, "cat")
      // (8, "computer")
      JavaPairRDD<Integer, String> wordLengths;

      // A groupByKey operation will collect all strings 
      // of the same length together, that is
      // (3, ["dog", "cat"])
      // (8, ["computer"])
      wordLengths.groupByKey();

  .. function:: reduceByKey(JFunction2<V, V, V>: func)

    Applies the given function to all the values with the same key.
    ``func`` **must** be commutative and associative, since there is no guarantee on the order of application.
    The classic word-count task makes for a good example::

      // You have a dataset of words and counts pairs, and 
      // you want to get the number of occurrences of a given word
      // across the entire dataset
      JavaPairRDD<String, Integer> wordsWithCounts;
      JavaPairRDD<String, Integer> counts = wordsWithCounts.reduceByKey((x, y) -> x + y)

    Note that the reduction function does not take as input the keys. 
    

  .. rubric:: Actions

  .. function:: collectAsMap()

    Collects the RDD locally as a ``java.util.Map<K, V>`` object.

    .. warning:: This method requires the driver to have enough memory to store the entire RDD at once. use this method only on small RDDs, otherwise you will get an ``OutOfMemoryError``.


.. class:: SparkContext

  .. function:: textFile(path)

    Reads a text file from the given path, returning its content as a ``JavaRDD<String>`` with one element per line.
