Third Homework
==============

.. highlight:: java

Preliminaries
-------------

You can download the `homework template <https://drive.google.com/uc?export=download&id=120llcNk2dAY1lKOLAxsKo8N-TxwHCuJB>`_
and place it in the project along with the other homeworks, renaming it as ``GxxH33.java``.
Along with this file, you should also download the class 
`InputOutput <https://drive.google.com/uc?export=download&id=1_2rTHh7loUpRg0V_TCqVIs5iJG4aQLJF>`_, which contains useful methods to load and write text files containing vectors.

Assignment
----------

The task of the third homework is to implement the sequential k-center algorithm by Gonzalez.
Points to be clustered are represented by instances of the class ``org.apache.spark.mllib.linalg.Vector``.
You can download a `zip file <https://drive.google.com/uc?export=download&id=1x1z4tgrEyBwv9jPto_iOiYo0FUJeheYp>`_ containing the following datasets:

* vecs-50-10000.bin: 10 thousands vectors in 50 dimensions
* vecs-50-50000.bin: 50 thousands vectors in 50 dimensions
* vecs-50-100000.bin: 100 thousands vectors in 50 dimensions
* vecs-50-500000.bin: 500 thousands vectors in 50 dimensions

These datasets are in text format and can be read using the ``InputOutput.readVectorsSeq`` method provided by the class `InputOutput`.

.. warning::
  There is also a ``org.apache.spark.ml.linalg.Vector`` class within Spark.
  They are functionally equivalent, but incompatible with one another.
  This unfortunate difference is due to the history of Spark's   API.
  For the homeworks we will use classes from the ``org.apache.spark.mllib`` package.

You have to develop a function that accepts a ``java.util.ArrayList`` of ``Vector`` objects and an integer ``k``, and returns the ``java.util.ArrayList`` of cluster centers (as ``Vector`` objects).
We will use the Euclidean distance between vectors.
The Spark library provides the static method ``sqdist`` in the class ``org.apache.spark.mllib.linalg.Vectors``, which returns the squared euclidean distance between two vectors.
Therefore, the following code returns the euclidean distance between two vectors ``a`` and ``b``::

  Vector a, b;
  double dist = Math.sqrt(Vectors.sqdist(a, b));

For your convenience, the homework template already contains a ``main`` method that takes two command line arguments, namely the path to the input and the integer ``k``, and runs your implementation of the algorithm.
It also measures the elapsed time, appending it to a file named ``k-center-time.txt``.

Exercises
---------

1. After implementing k-center, verify that it runs linearly in both ``k`` and ``n``. You can use the file ``k-center-time.txt`` for this purpose.
