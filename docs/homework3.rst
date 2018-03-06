Third Homework
==============

.. highlight:: java


The task of the third homework is to implement the sequential k-center algorithm by Gonzalez.
Points to be clustered are represented by instances of the class ``org.apache.spark.mllib.linalg.Vector``.
You can download three example datasets:

* `vecs-50-10000.bin <https://drive.google.com/uc?export=download&id=1b6G-Y7va7Lq7ikek5XoWEquy3hu51fl7>`_ 10 thousands vectors in 50 dimensions
* `vecs-50-50000.bin <https://drive.google.com/uc?export=download&id=1XZSi5jnijkwcXzhTkPS1cwyp-EN5-Ars>`_ 50 thousands vectors in 50 dimensions
* `vecs-50-100000.bin <https://drive.google.com/uc?export=download&id=15KHSg7J8W0qWzw1d9RTwjGejceVrwiCi>`_ 100 thousands vectors in 50 dimensions

These datasets are in binary format and should be read using the ``InputOutput.readVectors`` method provided within the project's template.

.. warning::
  There is also a ``org.apache.spark.ml.linalg.Vector`` class within Spark.
  They are functionally equivalent, but incompatible with one another.
  This unfortunate difference is due to the history of Spark's   API.
  For the homeworks we will use classes from the ``org.apache.spark.mllib`` package.

You have to develop a function that accepts a ``java.util.ArrayList`` of ``Vector`` objects and an integer ``k``, and returns the ``java.util.ArrayList`` of cluster centers (as ``Vector`` objects).
We will use the Euclidean distance between vectors, which is already implemented in the static method ``sqdist`` of the class ``org.apache.spark.mllib.linalg.Vectors``::

  Vector a, b;
  double dist = Vectors.sqdist(a, b);

For your convenience, the homework template already contains a ``main`` method that takes two command line arguments, namely the path to the input and the integer ``k``, and runs your implementation of the algorithm.
It also measures the elapsed time, appending it to a file named ``k-center-time.txt``.

Exercises
---------

1. After implementing k-center, verify that it runs linearly in both ``k`` and ``n``. You can use the file ``k-center-time.txt`` for this purpose.
