pack:
	cd code && zip -r\
		bd1718.zip build.gradle gradle gradlew gradlew.bat\
		src/main/java/it/unipd/dei/bd1718/FirstHomework.java\
		src/main/java/it/unipd/dei/bd1718/SecondHomework.java\
		src/main/java/it/unipd/dei/bd1718/ThirdHomework.java\
		src/main/java/it/unipd/dei/bd1718/FourthHomework.java\
		src/main/java/it/unipd/dei/bd1718/Utils.java\
		src/main/java/it/unipd/dei/bd1718/InputOutput.java

start-spark:
	ansible -b -i hosts minion-1,minion-2,minion-3,minion-4,minion-5,minion-6,minion-7,minion-8,minion-9 -a "/opt/spark/sbin/start-slave.sh spark://frontend:7077"

