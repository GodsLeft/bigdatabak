#!/usr/bin/env bash

# 这个文件将分离出来的文件转化为dot文件
while [ true ];do
    #sleep 2
    while [ -d streaming/_temporary ];do
        sleep 0.1
    done

    if [ ! -d streaming/_temporary ]; then
        rm streamingdot/part-00000
        cp streaming/part-00000 streamingdot/part-00000

        # 这边还缺少一个判断srcdst.dot文件是否存在的判断
        if [ ! -e streamingdot/srcdst.dot ]; then
            echo "digraph srcdst{" > streamingdot/srcdst.dot
            echo 'graph[bgcolor="transparent"]' >> streamingdot/srcdst.dot  # 想让背景透明
            echo "}" >> streamingdot/srcdst.dot
        fi

        sed -i '$d' streamingdot/srcdst.dot  # 刪除最後一行
        cat streamingdot/part-00000 >> streamingdot/srcdst.dot  # 將新文件添加到dot文件之後
        echo '}' >> streamingdot/srcdst.dot  # 添加最後一行

        linenum=$(cat streamingdot/srcdst.dot | wc -l)
        savelines=25
        if [ $linenum -gt $savelines ]; then
            decnum=$(($linenum - $savelines))
            sed -i "3,${decnum}d" streamingdot/srcdst.dot
        fi
        #dot -Tsvg streamingdot/srcdst.dot -o streamingdot/srcdst.svg
        dot -Tpng streamingdot/srcdst.dot -o streamingdot/srcdst.png

        if [ -f streamingdot/srcdst.png ]; then
            cat streamingdot/srcdst.png | nc 10.10.80.167 20000  # 这里的ip地址是内部ip地址
        fi
    fi


    #if [ ! -d streaming/_temporary ]; then
    #    rm streamingdot/srcdst.dot -f
    #    cp streaming/part-00000 streamingdot/srcdst.dot
    #    #sed -i '1i\size = "800, 800"' streamingdot/srcdst.dot
    #    sed -i '1i\digraph srcdst{' streamingdot/srcdst.dot
    #    echo '}' >> streamingdot/srcdst.dot
    #    dot -Tsvg streamingdot/srcdst.dot -o streamingdot/srcdst.svg

    #    if [ -f streamingdot/srcdst.svg ]; then
    #        cat streamingdot/srcdst.svg | nc slave04 20000 # 这里的slave04可以换成本机ubuntu的ip
    #    fi
    #fi
done