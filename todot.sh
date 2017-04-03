#!/usr/bin/env bash

# 这个文件将分离出来的文件转化为dot文件
while [ true ];do
    #sleep 2
    while [ -d streaming/_temporary ];do
        sleep 0.1
    done

    if [ ! -d streaming/_temporary ]; then
        rm streamingdot/srcdst.dot -f
        cp streaming/part-00000 streamingdot/srcdst.dot
        #sed -i '1i\size = "800, 800"' streamingdot/srcdst.dot
        sed -i '1i\digraph srcdst{' streamingdot/srcdst.dot
        echo '}' >> streamingdot/srcdst.dot
        dot -Tsvg streamingdot/srcdst.dot -o streamingdot/srcdst.svg

        if [ -f streamingdot/srcdst.svg ]; then
            cat streamingdot/srcdst.svg | nc slave04 20000 # 这里的slave04可以换成本机ubuntu的ip
        fi
    fi
done