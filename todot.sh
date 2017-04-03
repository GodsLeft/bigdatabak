#!/usr/bin/env bash

# 这个文件将分离出来的文件转化为dot文件
while [ true ];do
    sleep 2
    if [ ! -d streaming/_temporary ]; then
        rm streamingdot/srcdst.dot -f
        cp streaming/part-00000 streamingdot/srcdst.dot
        sed -i '1i\digraph srcdst{' streamingdot/srcdst.dot
        echo '}' >> streamingdot/srcdst.dot
        dot -Tsvg streamingdot/srcdst.dot -o streamingdot/srcdst.svg

        if [ -f streamingdot/srcdst.svg ]; then
            cat streamingdot/srcdst.svg | nc slave04 20000
        fi
    fi
done