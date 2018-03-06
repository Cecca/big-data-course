Preliminaries: setup
====================

.. highlight:: java

Before doing any work, you should setup your machine.
First of all, you need to have the Java Development Kit (JDK) version 8 installed on your machine.
If the command 

  javac -version

fails or returns something which is not along the lines of ``javac 1.8``, then head to `Oracle's download page <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`_ and download the Java Development Kit **version 8**.
Version 9 has problems with Spark, so we should avoid it for the time being.
If you are using Linux, you can instal the JDK 8 with your distibution's package manager.

Then you have to download the project template available `here <https://drive.google.com/uc?export=download&id=11REd6DsiYDTwnI8uWmoBh8q7FCd9ObAB>`_.
Unpack it somewhere on your filesystem.

Then, head over to the `download page of Intellij Idea <https://www.jetbrains.com/idea/download/>`_ and install it on your system.

The following is a step-by-step guide to importing the project into Intellij and configuring it for a first run.
On the startup screen, select ``Import Project``: use the file selection dialog that pops up to select the file ``build.gradle`` file contained in the directory you downloaded.

.. image:: images/idea-03.png

You will have to wait a couple of minutes until Intellij configures itself.
after that, use the project navigation panel on the left to open the first homework template.
If the editor looks like the following

.. image:: images/idea-06.png

then it means that there is no Development Kit associated to the project.
To fix this problem, open the menu ``File/Project structure``.
A window like the following will pop up

.. image:: images/idea-07.png

Click the ``New`` button and select ``JDK``

.. image:: images/idea-08.png

In the file selection dialog that shows up, select the JDK directory.
This is the directory containing the ``lib`` and ``bin`` folders that include Java executables and libraries.
Usually Intellij will suggest the right one.

.. image:: images/idea-09.png

At this point all errors will disappear.
On the line of the ``main`` method there is a green arrow, which allows you to run your code.

.. image:: images/idea-10.png

Clicking the green arrow will compile the code and run it.
The first time you try to run it will result in an error, since the program expects to receive a file name on the command line.
To configure the parameters with which the program is run, use the drop down menu shown below

.. image:: images/idea-12.png

This will open the following dialog

.. image:: images/idea-13.png

On the ``program arguments`` box you can set the arguments that are passed to the ``main`` method.
You should also set the value of ``VM Options`` as shown in the figure.
As you will see in the second homework, Spark can run both locally on your machine or on a cluster.
Which of the two configurations is used is determined by the ``spark.master`` `java system property <http://www.java2s.com/Tutorial/Java/0120__Development/SettingtheValueofaSystemPropertyfromtheCommandLineaddDoptiontothejavacommandwhenrunningyourprogram.htm>`_.

.. note:: A note for Python users

  Spark also exposes a Python interface. To use Spark from Python on your local machine, the simplest way is to install the ``pyspark`` package with the command::

    pip install pyspark

  Then in your Python script it's sufficient to add the following statement on top of the file::

    import pyspark

  You can find more about Spark's Python API `here <https://spark.apache.org/docs/latest/api/python/>`_.
