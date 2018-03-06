Homework 1: functional programming
==================================

.. highlight:: java


The purpose of this first homework is to get acquainted with the principles of *functional programming*, on which MapReduce and Spark are based.
One of the core ideas of functional programming is that functions can be arguments to other functions.
For instance, a sorting algorithm may take as a parameter the comparison function along with the data to be sorted.

Java 8 introduced support for this style of programming by adding new syntax for specifying so-called *anonymous functions*, also called *lambdas*.
This syntax allows to write functions directly in the argument list of other functions.
The syntax for specifying a function is the following::

  (T1 param1, T2 param2, ...) -> {
    // Body of the function
    // with as many statements as you need
    // separated by semicolons, just like regular
    // Java statements.
    return /* possibly something */;
  }

Where ``T1``, and ``T2`` are the types of ``param1`` and ``param2``, respectively.

If the function is made by a single statement, a more concise syntax can be used::

  (T1 param1, T2 param2) -> /* single statement with no semicolon */

the result of the single statement will be the return value of the function.

If the type of the parameters can be inferred from the context, it can be omitted.

An example will make things clearer.
Imagine you have a collection ``coll`` of ``Double`` with a method ``map`` (more on such collections later).
The ``map`` method transforms the collection in a new one by applying the a function, passed as a parameter, to each element.
Therefore, to obtain a a collection of the squared values you should do the following::

  coll.map((Double x) -> x*x);

Since the collection is of ``Double``, the compiler can infer the type of ``x``, so in this case we can write::

  coll.map((x) -> x*x);

To make another example, imagine that you want to transform your collection of ``Double`` into a collection of differences from some other value, defined in a variable::

  double fixed = 1.5;

  coll.map((x) -> {
    double diff = fixed - x;
    return diff;
  });

Note that ``fixed`` is used in the body of the anonymous function, but is defined outside of it!
In such cases we say that the anonymous function *captures* a variable.
You cannot re-assign a captured variable within an anonymous function.
Trying to do it will result in a compilation error mentioning that all captured variables must be *effectively final*, which is the compiler's way of saying that you cannot re-assign them.

Java 8 also introduced another way of passing functions to other functions, namely *method references*.
Suppose you have the following class::

  public class Operations {

    public static double square(double x, double y) {
      return x * y;
    }
  }

You may which to pass the *static* method ``square`` to the method ``map`` instead of defining a lambda function, like in the examples above.
The syntax to refer the the static method ``square`` is the following::

  coll.map(Operations::square);

note the double colon joining the method name ``square`` to the class it belongs to, ``Operations``.

Therefore, you have two ways of passing a function to a method: either you pass an anonymous function or a method reference.
Usually, lambda functions are used when the functionality can be coded in a few statements and is limited to a single occurrence.
Method references, on the other hand, are useful when the code gets more complex or when it should be reused in several places.


So far, we have assumed the existence of a collection type providing a ``map`` method accepting a function as an argument.
There are several types of collections providing such a method (including many from the Java Standard Library).
However, since these homeworks are about Spark, we will focus on collections provided by Spark, namely *Resilient Distributed Datasets* (RDD for short).
An RDD is a collection of elements that can be possibly partitioned across many machines and on which operations execute in parallel.
In the Spark Java API, the class defining the RDD data structure is ``JavaRDD`` (`API link <https://spark.apache.org/docs/latest/api/java/org/apache/spark/api/java/JavaRDD.html>`_).

The peculiarity of the RDD data structure is that it does not allow in-place updates.
The only way to modify the contents of an RDD is to *transform* it in another collection by means of some method.
Some methods to transform an RDD into another are the following:

* ``map``: yields another RDD by applying the supplied function on each element
* ``filter``: returns an RDD containing only the elements for which the given boolean function returns ``true``.

There are also functions to get a single value which is the result of some operation on the entire collection, which are called *actions*:

* ``count``: returns the number of elements of the RDD
* ``collect``: store all the data of the RDD in a local ``List``.
* ``reduce``: returns the result of *reducing* the collection with the given commutative and associative function. Conceptually, this operation is equivalent to applying the function to the first two elements of the collections, then to the result and the third, then to the result and the fourth and so on, until there are no more values.
  No assumptions on the order of applications can be made, which is why the function needs to be associative and commutative. The following picture depicts an example for the addition function on the list ``47 11 42 13``

  .. image:: https://i.stack.imgur.com/OCsJC.png

These are not all the methods available to transform and collect data in Spark: an overview of the most useful methods and types is given :ref:`here <spark-api>`.

Note that the methods of ``JavaRDD`` that we saw are all functional in nature (well, except ``count`` and ``collect``): they accept another function as a parameter to know what to do with elements.

.. sidebar:: Variable names

  In the example code we will tend to use variable names with one-letter prefixes to distinguish variables representing local data and distributed data (that is, RDD data). Local data will be prefixed by ``l`` (e.g. ``lName``) and distributed data by ``d`` (e.g. ``dName``).

Now, open the file ``src/main/java/it/unipd/dei/bd1718/FirstHomeworkTemplate.java`` in the project template.
It contains some setup code (most of which will be explained in the next homework) and a section where you can put your code.
The setup code reads a sequence of ``double`` values from the command line (see the video in the :ref:`preliminaries <preliminaries>` section to see how to configure command line arguments in Intellij IDEA) and turns them into a ``JavaRDD`` named ``dNumbers``.

We will develop some code that computes the sum of squares of the elements of ``lNumbers``. You can use this code as a starting point for this homework's assignment.

.. code-block:: java

  // Compute the square of each number, obtaining a new JavaRDD
  JavaRDD<Double> dSquares = dNumbers.map((x) -> x*x);
  // Reduce dSquares to a single number
  double sumOfSquares = dSquares.reduce((x, y) -> x + y);

  System.out.println("The sum of squares is: " + sumOfSquares);


Exercises
^^^^^^^^^

Explore Spark's API by coding variations of the examples we made above.
Here are some examples: 

1. Given ``dNumbers``, compute the absolute value of the difference between each element and the mean of all the values.

2. Given ``dNumbers``, compute the minimum of all the inverse values. Do it in at least two ways (e.g. using ``map`` and ``reduce``, and using the ``min`` method with an anonymous function implementing a custom comparator. The ``min`` operator has some caveats, have a look :ref:`here <spark-api>`).

Come up with a third task. You can use whichever methods of ``JavaRDD`` you want.
