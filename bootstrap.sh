#!/bin/sh
#service iptables stop
cd `dirname $0`
mkdir -p log/sasl
exec erl -sname akita -pa $PWD/ebin $PWD/deps/*/ebin -setcookie akita -boot start_sasl -config sys.config -hidden
