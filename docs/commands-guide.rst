Basic commands
==============

This page summarizes the commands needed to run jobs and manage data.

Job submission
--------------

.. _spark-submit:

.. function:: spark-submit [spark options] code.jar [program arguments]

  Submit Spark jobs to the cluster. This command requires the path to the jar file containing **all** the bytecode of your application.
  How to build such a file is explained in the fourth homework.
  
  This command has many parameters, which can by shown by invoking::

    spark-submit --help

  Among the options that you can set, the following are of primary interest in our case:

  * ``--class``: the class containing the ``main`` method you intend to run. This argument is mandatory.
  * ``--num-executors``: the number of executors, that is, the number of Java virtual machines that you want to run jour job.
  * ``--executor-cores``: the number of cores to be used by each executor. Each executor can have at most 8 cores.
  * ``--executor-memory``: the amount of memory to be used by each executor (maximum 2G)

  There is also an option ``--master`` that allows to select the Spark master to which send the job. On the cluster this is already configured to ``yarn``, so you don't need use this option.

.. topic:: Cluster managers

  Spark supports the execution of jobs using several cluster managers (for more information please refer to the `official documentation <https://spark.apache.org/docs/latest/submitting-applications.html>`_).
  For the Big Data course we are using the `Yarn <https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html>`_ cluster manager.

Data management
---------------

Data in the cluster lives on HDFS, the Hadoop distributed filesystem.
Spark is already configured to read data from HDSF.
The command to interact with HDFS is called ``hdfs``.

.. _hdfs-command:

.. function:: hdfs <subcommand> [options]

  The hdfs command has several subcommands, most of which can be used only by the cluster's administrator.
  The most interesting subcommand for normal operation is ``hdfs dfs``, which allows to list, move, copy, remove, upload, download, etc.. files and directories in HDFS. Here is a brief synopsis of the most useful options.

  * ``-ls`` Lists the file in the given directory. If no directory is given, then it shows files under ``/user/groupXX`` if your username is ``groupXX``.
  * ``-rm`` Removes the given file. Paths not beginning with a ``/`` are relative to ``/user/groupXX``. If you need to remove a directory, you should also pass the options ``-r``.
  * ``-mv`` Move a file or directory to another name.
  * ``-get`` Downloads the given file to the local filesystem.
  * ``-put`` Uploads the given file from the local filesystem.