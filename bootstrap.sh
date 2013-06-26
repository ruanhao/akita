#!/bin/sh
#service iptables stop
cd `dirname $0`
mkdir -p log/sasl
exec erl -sname akita -pa $PWD/ebin -setcookie akita -boot start_sasl -config sys.config -s akita
