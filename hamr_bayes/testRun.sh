
rm -f *.class
javac -cp $EXAMPLES_CP Zword.java StoreList.java

#time hamr horn -m master -s master -cp . Zword -z /home/hadoop/hadoop-2.4.1 -h zhu_disk/HS/Ham_1 -s zhu_disk/HS/Spam_1 -o zhu_disk/out_1

for k in $( seq 1 19 )
do
    for t in $( seq 1 3 )
    do
        ( time hamr horn -m master -s master -cp . Zword -z /home/hadoop/hadoop-2.4.1 -h zhu_disk/HS/Ham_${k} -s zhu_disk/HS/Spam_${k} -o zhu_disk/out_${k} ) 2>> time_out
    done
done

cat time_out | grep real > time_out_real
rm -f time_out

