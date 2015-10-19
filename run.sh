rm -f  *.class

javac -cp $EXAMPLES_CP Zword.java StoreList.java

#hamr horn -m master -s master -cp . Zword -z /home/hadoop/hadoop-2.4.1 -h zhu_disk/input/file01.txt -s zhu_disk/input/file02.txt -o zhu_disk/xx

hamr horn -m master -s master -cp . Zword -z /home/hadoop/hadoop-2.4.1 -h zhu_disk/ham/bigHam -s zhu_disk/spam/bigSpam -o zhu_disk/xx