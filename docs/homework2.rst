Homework 2: Spark basics
========================

In this second homework, we will see how to use Spark more in detail, using the classic word count task as a running example.

.. highlight:: java

Preliminaries: RDDs, executors, and parallelism
----------------------------------------------

Before proceeding to homework 2, some words about Resilient Distributed Datasets, Spark's basic data abstraction, are in oder.

A Resilient Distributed Dataset (RDD for short) is a collection of elements of the same type, possibly distributed across many machines.
As we have briefly discussed in the previous homework, a RDD cannot be modified in place.
Instead, new RDDs are created by means of *transformations*: we provide a function, and Spark applies it to the elements of the RDD, yielding another RDD with the resulting values.
An RDD is partitioned in a configurable number of *blocks*, each of which contains several elements.
The blocks can be distributed across many machines.

Each machine is called an *executor* in Spark, and usually has many cores.
When transforming an RDD, we have a function to be applied to all the elements of the RDD.
In Spark, a *task* is a pair formed by a function and a block of a RDD.
Therefore, when transforming an RDD, we have as many tasks as RDD blocks.
Each core of each executor will take a task and execute it: the function will be applied *sequentially* to each element of the given RDD block.
Therefore, parallelism arises from the fact that the RDDs are partitioned in many blocks, and each block can be processed independently.
Furthermore, note that the number of blocks and the total number of cores may be different:

* you may have more cores than blocks, in which case there are not enough tasks and some cores will sit idle
* you can have exactly as many blocks as cores. Each core usually executes a single task. This actually depends on the scheduler, which may opt to make some cores execute more than one task and leave some others with no work.
* you can have more blocks than cores, in which case some tasks will wait for others to finish.

Setting the number of blocks depends on the application. Sometimes having many more blocks than cores helps with load balancing: cores completing tasks faster (possibly because of the characteristics of the data they are processing)  will have more tasks to work on.

--------------------------------------------------------------------------------


Setting things up: the Spark context
------------------------------------

The entry point to Spark is the Spark context. 
Since Spark can run on your laptop and on many different cluster architectures, to simplify the user experience Spark developers have created a single entry point that handles all the gory details behind the scenes.
In the Java API, the relevant class is ``JavaSparkContext``.
To instantiate such a class, you need to provide some configuration using the class ``SparkConf``::

  SparkConf configuration = 
    new SparkConf(true)
      .setAppName("application name here")
      .setMaster("<master>");

Let's break down the code snippet above.
On line 2, we pass ``true`` to the ``SparkConf`` constructor.
The effect is that configuration properties will be read from system properties (i.e., the ones passed on the command line after the ``java`` command using the ``-Dproperty.name=property-value`` sintax).
Line 3 sets the name of your application. Note that this line and the following one are method invocations on the ``SparkConf`` object being created.
Finally, line 4 sets the address of the master.
As detailed in the `Spark documentation <https://spark.apache.org/docs/latest/submitting-applications.html#master-urls>`_, there are several values that this string can take.
For this course, two are interesting.

* ``"local[*]"``: use the local resources of the computer. This sets up a Spark process on the local machine, using the available cores for parallelism.
  Use this setting when testing code on your local machine.
* ``"yarn"``: run Spark on the Yarn cluster manager. This is the cluster manager used by the cloud computing platform available for the course. Use this setting when running on it.

There is also the possibility of not setting the master in the ``SparkConf`` object. 
In this case, you should specify the Spark master either using the Java property ``spark.master`` on the command line (for instance when running locally on your laptop), or by specifying the ``--master`` option of the ``spark-submit`` command (:ref:`documentation <spark-submit>`).
By not hardcoding the master configuration in you code, you have the flexibility of running on different architectures.
If you are using the Intellij Idea IDE, you can configure the Spark master using the configuration dialog that can be accessed from ``Run -> Edit configurations``, as shown in the following figure, where the relevant configuration is ``VM options``.

.. figure:: images/configure-master.png

A run configuration is created for you the first time you try to run a main method by clicking on the green arrow beside the line of the main method itself.

Once you have created a ``SparkConf`` object, you can instantiate a ``JavaSparkContext`` as simply as::

  JavaSparkContext sc = new JavaSparkContext(configuration);

Now we are ready to use this Spark context to load data.

Loading data from text files
----------------------------

In the first homework, we built a RDD by calling ``sc.parallelize`` on an existing collection.
However, usually data is stored in one or more files, in a variety of formats.
The simplest format is plaintext, we we will now see how to load text files into Spark.
Download the sample file `text-sample.txt <https://drive.google.com/uc?export=download&id=1DWSGmHHepOfAUznj5KrfU9q4kg-tlslY>`_ and place it in the root directory of your code.

The following line of code loads the text file into an RDD with an element for each line::

  JavaRDD<String> lines = sc.textFile("text-sample.txt");

If you open the file you will see that it looks quite strange. Here are the first three lines::

  Follyfoot Follyfoot be a child television series co-produce by the majority-partner british television company Yorkshire Television for transmission on itv and the independe...
  Golden line the golden line be a type of Latin dactylic hexameter frequently mention in Latin classroom in English speak country and in contemporary scholarship write in Eng...
  Chris Isaak Christopher Joseph Isaak bear June be a american rock musician and occasional actor he be best know for he hit wicked game as well as the popular hit song Baby...

This file has been obtained by sampling 1000 documents from a recent dump of Wikipedia.
The text of each page has been `lemmatized <https://en.wikipedia.org/wiki/Lemmatisation>`_. Lemmatization is a common preprocessing step when dealing with text documents.
Using Natural Language Processing techniques, each word is reduced to a *lemma*: plurals are turned to singluar, verbs are turned to infinity, etc...

In this homework, in which we are counting word occurrences, it is useful to have a lemmatized dataset: this way we count variations of the same lemma correctly.
    

.. topic:: Profiling

  When writing code, one often wonders how much time the program is taking, and where the time is spent.
  In sequential programs, measuring time is as simple as calling ``System.currentTimeMillis()`` in the relevant spots.
  In Spark programs, however, there are some issues with this approach.
  Spark transformations are *lazy*, in the sense that they don't happen right away, when a transformation method is called. 
  Instead, Spark remembers that it has to transform the data with the given function.
  The transformation will be actually executed only once an action (such as counting the elements or writing them to a file) requires the transformed data.
  This allow Spark to run more efficiently, however it makes measuring time more difficult.
  Suppose we want to take the time it takes to count the words of :file:`text-sample.txt` using the methods we will see in the next section.
  If we were to do like the following::

    JavaRDD<String> docs = sc.textFile("text-sample.txt");
    long start = System.currentTimeMillis();

    // Code of which we want to measure the running time

    long end = System.currentTimeMillis();
    System.out.println("Elapsed time " + (end - start) + " ms");

  then we would be measuring also the time to load the text file!
  In fact, ``sc.textFile`` is not executed immediately, rather it is executed when an action requires it, *after* we start our stopwatch.
  Therefore, we need to *force* the file loading before we start the stopwatch.
  In order to do so, we have to run an action on the ``docs`` RDD, and the simplest one is ``count``.
  However, simply invoking ``count`` would not do: we have to explicitly tell Spark to cache the results in memory::

    JavaRDD<String> docs = sc.textFile("text-sample.txt").cache();
    docs.count();

    // Now the RDD has been loaded and cached in memory and
    // we can start measuring time
    long start = System.currentTimeMillis();

    // Code of which we want to measure the running time

    long end = System.currentTimeMillis();
    System.out.println("Elapsed time " + (end - start) + " ms");

  The above strategy is good to take the overall running time of a section of the program, but is inadequate for finer grained profiling.
  To see how much time your Spark program spends running each transformation and action, you can use the web interface that is built-in into Spark.
  This interface runs alongside your program, and exits when the program terminates.
  In order to have time to consult it, we have to suspend the execution of the program.
  The simplest way is by inserting an input statement right before the end of your ``main`` method::

    System.out.println("Press enter to finish");
    System.in.read();

  Now, after starting your program, open a browser and visit `<localhost:4040>`_.
  You will see the web interface of your running program, which you are invited to explore.
  In the fourth homework (where we will run software on the cloud) we will see an alternative way of accessing the web interface after your program has terminated, allowing you to get rid of the ``System.in.read()`` call. 


Counting words
--------------

A classic MapReduce example is word count.
Given a set of documents, we want to count the number of occurences of each word across all the document corpus.
We will see several ways to implement it in Spark.
Each implementation will assume that we have a ``docs`` collection of type ``JavaRDD`` where each line is a sequence of space-separated words prepresenting a document.
Such a dataset can be obtained from the :file:`text-sample.txt` file with::

  JavaRDD<String> docs = sc.textFile("text-sample.txt");

First of all, let's look at the classic MapReduce algorithm for word counting::

  Map<String, Long> count = docs
    .flatMapToPair((document) -> {             // <-- Map phase
      String[] tokens = document.split(" ");
      ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
      for (String token : tokens) {
        pairs.add(new Tuple2<>(token, 1L));
      }
      return pairs.iterator();
    })
    .groupByKey()                       // <-- Reduce phase
    .mapValues((it) -> {
      long sum = 0;
      for (long c : it) {
        sum += c;
      }
      return sum;
    })
    .collectAsMap();    // <-- Collection of the result

The map function is implemented with a ``flatMapToPair`` call.
We use a ``flatMapToPair`` operation for two reasons.
The first is that we have to generate a dataset of key-value pairs, the second is tat from a single value (a document represented as a single string), we have to generate several key-value pairs.
The map function first splits the document at spaces then iterates over the tokens, creating a new pair for each one of them.
The pairs are accumulated in an ArrayList, which we then return as an iterator.

For the reduce phase, we have to use a chain of two functions.
First, with a ``groupByKey`` operation we collect the values (that is the counts) associated to the same key (which is a word) into a list.
Then, with a ``mapValues`` operation we can apply a function to all the values associated to a key.
The function we apply simply accumulates all the counts into a ``sum`` variables, which is then returned as the result.

This implementation takes **6073 milliseconds** on average, on my machine using 4 cores.

Observe that this implementation is missing the opportunity of some optimizations.
Most likely there are several words that occur more than once in a single document.
As of now, we are creating a key-value pair with value ``1L`` for each occurrence of repeating words.
What if we accumulate counts of repeated occurences during the map phase instead?

Try to change the function passed to ``flatMapToPair`` to the following::

    (document) -> {
      String[] tokens = document.split(" ");
      HashMap<String, Long> counts = new HashMap<>();
      ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
      for (String token : tokens) {
        counts.put(token, 1L + counts.getOrDefault(token, 0L));
      }
      for (Map.Entry<String, Long> e : counts.entrySet()) {
        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
      }
      return pairs.iterator();
    }

You should see an improvement in the running time.
On my machine, this version takes **4390.2 milliseconds** on average.
This improvement is due to the fact that we are doing *document-level* aggregation, that is, we are aggregating some data before information is exchanged between processors, thus reducing the amount of data exchanged.

We can bring this concept a step further.
Recall that a RDD is a collection of data partitioned across many processors, potentially located on different machines.
Each partition will contain several elements, documents in this case, and will be processed in one go by a single processor.
So, instead of doing document-level aggregation, we could do partition-level aggregation.
This is the purpose of the ``reduceByKey`` operation.
The name is slightly misleading, since it's not really equivalent to the standard definition of a reduce operation in MapReduce.
The standard reduce of MapReduce can operate on all the values associated with a key, and is implemented with a ``groupByKey`` in Spark, as we have seen.
A ``reduceByKey``, instead, applies a user-provided function to pairs of elements sharing the same key, until there is just one element per key.
There is no guarantee on the order of application, therefore the function being applyed **must** be commutative and associative.
By accepting this restriction, you allow Spark to aggregate data at a partition level, resulting in even less communication between processors and therefore in more performant code.
The implementation of word count with ``reduceByKey`` is the following::

  Map<String, Long> count = docs
    .flatMapToPair((document) -> {   // <-- map phase
      String[] tokens = document.split(" ");
      HashMap<String, Long> counts = new HashMap<>();
      ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
      for (String token : tokens) {
        counts.put(token, 1L + counts.getOrDefault(token, 0L));
      }
      for (Map.Entry<String, Long> e : counts.entrySet()) {
        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
      }
      return pairs.iterator();
    })
    .reduceByKey((x, y) -> x + y)   // <-- reduce phase
    .collectAsMap();

On my machine, using 4 cores, this implementation takes **3641.2 milliseconds** on average.

The plot below compares the running times of the three implementations we have seen so far, with confidence bars.

.. figure:: images/word-count-implementations.png

Each bar corresponds to a different implementation:

1. ``gropuByKey`` is the first implementation we looked at.
2. ``groupByKey-agg`` is the second implementation, with document-level aggregation.
3. ``reduceByKey`` is the last implementation, doing partition-level aggregation.

Exercises
---------

1. Compute an histogram of word lengths: the number of words of length 1, 2, 3, and so on...

