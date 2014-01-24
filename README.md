akita
=====

Purpose  
-------
Elang cluster information recorder  
Take a snapshot of the Erlang system at time slots  

Version  
-------
0.2.0  

Characters  
-----------
Monitor performance of an Erlang distributed system  
Collects CPU, memory and Erlang process information periodly  
Save top N processes according to memory, reduction and message queue  

Change Log  
----------
Introduce application [westminster] to setup Erlang network underlying  
Provide interface to show tool status live  
Enhance robustness for node down and process terminates unexpectly  
Create a server to collect dets file from every Erlang node  
Present dets content in both TXT and CSV format  

How to use  
----------  
#### Pull westminster
`rebar get-deps`

#### Deploy westminster
[see how]  

#### Make Configuration
in akita/src/akita.app.src:  
```erlang
{application, akita,
    [
    ...
    {env, [{interval, 1000}, %% collection period
           {topn, 5},        %% top n processes to save
           {smp, true}]}]}.  %% is SMP enabled
```
#### Compile
`rebar compile`

#### Start akita Erlang shell
`./bootstrap`

#### Key API
`akita:start().` -> start akita  
`akita:start_collect().` -> start collecting info on every node  
`akita:stop_collect().` -> stop collecting world-wide  
`akita:status().` -> show akita runtime status  
`akita:pull().` -> pull dets files from all nodes  
`akita:process(Dir).` -> produce txt and csv file  
`akita:start(offline).` -> start akita in offline mode just in order to collect dets files  

Sample
------
#### shell console
```shell
=INFO REPORT==== 24-Jan-2014::09:50:30 ===
CURRENT COLLECTORS: 
hello@hao
----------------------------
IS COLLECTING NOW: 
true
----------------------------
START COLLECTING: 
 1/24/2014  9:50:18
STOP COLLECTING: 
 undefined
----------------------------
CONFIGS: 
interval: 1000
topn: 5
smp: true
----------------------------
OFFLINE: 
false
ok
```
#### .txt:  
```erlang
{{epoch,{{2013,8,19},{20,50,31}}},
 {core,-1},
 {cpu_util,-1},
 {mem_util,-1},
 {mem_toplist,[<7181.32.0>,<7181.11.0>,<7181.25.0>,<7181.3.0>,<7181.14.0>]},
 {red_toplist,[<7181.3.0>,<7181.25.0>,<7181.11.0>,<7181.67.0>,<7181.40.0>]},
 {mq_toplist,[<7181.80.0>,<7181.79.0>,<7181.78.0>,<7181.77.0>,<7181.76.0>]},
 {procs_info, ... %% omitted
```  
#### .csv:  
akita provides csv file to observe CPU and memory visually  
When there is something suspect, you can trace it in corresponding txt file by the clue of **epoch**
![csv][1]  

Acknowledge
-----------
Fiona, my wife who is cooking while i am coding  

akita is absolutely good
------------------------
You owe me a cup of Pepsi  
[![Donate]](http://goo.gl/6zcOL)  

  [westminster]:  https://github.com/ruanhao/westminster.git
  [see how]:  https://github.com/ruanhao/westminster.git
  [Donate]:  https://www.paypal.com/en_US/i/btn/btn_donate_SM.gif
  [1]:  https://raw.github.com/ruanhao/akita/master/sample/sample.png
