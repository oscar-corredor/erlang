#!/bin/sh
erl -noshell -s $1 $2 -s init stop
