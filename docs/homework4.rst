Fourth Homework
===============

.. highlight:: java


This homework's task is to solve the diversity maximization problem you have seen in class.
The homework template provides the implementation of the sequential approximation algorithm for diversity maximization, along with a method to compute the diversity of a given set.
Furthermore, there is a ``main`` method which takes care of reading command line arguments, loading input, and so on.
The ``main`` method also allows you to select which algorithm to run on the input: if the sequential one, a random sampling or your MapReduce implementation.

You are required to implement two methods.
The first method, ``runRandom``, should take a RDD of vectors and return a random sample of size ``k``, to serve as a baseline.
The second method, ``runMapReduce``, takes as input a RDD of vectors, the parameter ``k``, and the number of blocks on which to apply the k-center algorithm you developed in the third homework.
You can group by random keys using a combination of the ``groupBy`` key method of the ``JavaRDD`` class, and the ``java.util.Random.nextInt(int)`` method.

For the purpose of testing your implementation on your laptop, you can use the sample of vectors provided for the previous homework.
For this homework, you should also run your code on the cluster.
To pack your code in a jar file suitable to be uploaded on the cluster, you can follow the steps below.

First, open the ``gradle`` panel by hovering over the menu in the bottom-left corner

.. image:: images/shadow-jar-1.png

Then run the ``shadowjar`` task, which will create a jar file containing all your code and its dependencies in the directory ``build/libs``

.. image:: images/shadow-jar-2.png

Then, to upload the jar file to your account on the cluster, open the embedded terminal, again by hovering over the bottom-left button.

.. image:: images/shadow-jar-3.png

The terminal will open in the root directory of the project.
Run the scp command as shown in the image below, changing ``group01`` to your group's ID, and possibly changing the name of the jar file.

.. image:: images/shadow-jar-4.png

If you are on windows, replace ``scp`` with ``pscp``, and use ``\`` instead of ``/`` in file paths.
On all operating systems, don't worry if you don't see characters appearing on screen while you type your password: it's the expected behaviour to preserve your privacy.

Now you can login to the ``frontend`` and run your code using the ``spark-submit``, which is described :ref:`here <spark-submit>`.
Data on the cluster is hosted in `HDFS <https://www.ibm.com/analytics/hadoop/hdfs>`_, which stands for Hadoop Distributed Filesystem.
Just like the filesystems you are used to, HDFS is organized in files and directories.
Therefore, on the cluster there are two co-existing file hierarchies: the Operating System one, which is used during normal operation, and HDFS, which stores data to be used as input for Spark jobs.
To interact with HDFS there is a dedicated command, unsurprisingly called ``hdfs``, whose synopsis is given :ref:`here <hdfs-command>`.
In particular, there is directory named ``/data`` in HDFS which contains read-only copies of datasets that you can use to test your code.
To get a list of the available datasets you can use the following command::

  hdfs dfs -ls /data

All datasets available under ``/data`` are made of ``Vector`` instances, with each vector having the number of dimensions specified by the first number its name. The second number is the number of vectors in the dataset.
So, for instance, the dataset ``vectors-50-1000000`` is made by one million 50-dimensional vectors.
Files with ``all`` in place of a number (like ``vectors-50-all``) are made by approximately 5 million vectors and correspond to the original dataset of which the others are samples.
An example invocation of the fourth homework on the cluster is as follows::

  spark-submit --num-executors 2 --class it.unipd.dei.bdc1718.FourtHomework bdc1718-all.jar --input /data/vectors-50-1000000 -k 10

.. warning::

  The ``/data`` directory in HDFS is read only: if you try to write into ``/data`` you will get an exception.
  Each group (for instance ``groupXX``) has write access to a folder in HDFS called ``/user/groupXX`` with a 10GB quota.
  This is also the default folder for HDFS if you provide relative paths (i.e. paths not starting with a ``/`` to its commands).

If you want to play with other datasets, you can upload them to the cluster using the ``scp`` command (``pscp`` on Windows) just like you did for the jar file.
To make the datasets available to Spark, you have to place them in HDFS.
Assuming that the dataset is called ``dataset.txt`` and your group name is groupXX, running the following command::

  hdfs dfs -put dataset.txt

will make the file available to Spark under the HDFS path ``/user/groupXX/dataset.txt``.

.. note::

  Exceptions with a message like "Missing an output location for shuffle X" are likely due to insufficient memory. Try to look in the log for error messages mentioning "Container killed by YARN for exceeding memory limits".  
  If this is the case, try to give more memory to each executor, or to use more executors.

.. warning::

  In order to share the limited computing resources of the cluster among all the groups, each job can request at most 2Gb of memory *per executor* (using the ``--executor-memory`` flag of ``spark-submit``.
  If you need more memory you can require more executors (using the ``--num-executors`` flag).
  If your program fails with a message similar to the following::

    Exception in thread "main" java.lang.IllegalArgumentException: Required executor memory (3072+384 MB) is above the max threshold (2432 MB) of this cluster!

  then it means that you have requested too much memory for single executors (in this case 3Gb).


Exercises
---------

1. Verify if the diversity of the solution found by the algorithm is significantly larger than that of a random sample.
2. Study the scalability of your implementation with respect to the various parameters, running on the cluster.
