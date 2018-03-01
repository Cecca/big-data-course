Basic commands
==============

This page summarizes the commands needed to run jobs and manage data.

Job submission
--------------

To run your code on the cloud you will first need to have it packaged in a "fat jar", that is a jar file containing all the dependencies. You can find instructions on how to build such a file `here <TODO>`_

These commands live in ``/opt/spark/bin``

.. sidebar:: Cluster managers

  Spark supports the execution of jobs using several cluster managers (for more information please refer to the `official documentation <https://spark.apache.org/docs/latest/submitting-applications.html>`_).
  For the Big Data course we are using the `Yarn <https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html>`_ cluster manager.

.. _spark-submit:

.. function:: spark-submit [spark options] code.jar [program arguments]

  Submit Spark jobs to the cluster. This command requires the path to the jar file containing **all** the bytecode of your application [#building-jar]_.
  
  This command has many parameters, which can by shown by invoking::

    spark-submit --help

  Among the options that you can set, the following are of primary interest in our case:

  * ``--class``: the class containing the ``main`` method you intend to run. This argument is mandatory.
  * ``--num-executors``: the number of executors, that is, the number of Java virtual machines that you want to run jour job
  * ``--executor-cores``: the number of cores to be used by each executor (TODO: max, min)
  * ``--executor-memory``: the amount of memory to be used by each executor (TODO: max, min)


  TODO: Say something of ``--master``

Data management
---------------

Data in the cluster lives on `HDFS <https://www.google.it/search?client=ubuntu&channel=fs&q=hdfs&ie=utf-8&oe=utf-8&gfe_rd=cr&dcr=0&ei=sqBYWvzAM7TBXvHopIAH>`_, the Hadoop distributed filesystem.

Commands to manage data in HDFS live in ``/opt/hadoop/bin``.

.. _hdfs-command:

.. function:: hdfs <subcommand> [options]

.. rubric:: Footnotes

.. [#building-jar] You can build such a jar by using the ``./gradlew shadowjar`` command on your machine

