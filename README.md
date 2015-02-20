# Aeolus
Aeolus is a batching and optimization framework on top of [Apache Storm](https://storm.apache.org/) (see also https://github.com/apache/storm).

Aeolus was originally developed at [HP Lab](http://www.hpl.hp.com/) (Palo Alto, CA) in colaboration of [Humbuldt-Universität zu Berlin](https://www.hu-berlin.de/?set_language=en&cl=en) ([DBIS Group](http://www.dbis.informatik.hu-berlin.de/index.php?id=5&L=1)). This is a re-implementation inspired by the following publications:
* [Building a Transparent Batching Layer for Storm](http://www.hpl.hp.com/techreports/2013/HPL-2013-69.html) (HP Labs Technical Report)
* [Performance Optimization for Distributed Intra-Node-Parallel Streaming Systems](https://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=6547428&url=http%3A%2F%2Fieeexplore.ieee.org%2Fxpls%2Fabs_all.jsp%3Farnumber%3D6547428) (SMDB WS at ICDE 2013)
* [Aeolus: An Optimizer for Distributed Intra-Node-Parallel Streaming Systems](https://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=6544924&url=http%3A%2F%2Fieeexplore.ieee.org%2Fxpls%2Fabs_all.jsp%3Farnumber%3D6544924) (Demo at ICDE 2013)

Additionally, this framework contains Storm Topologies for benchmarking purpose:
* [Linear Road Benachmark](http://www.cs.brandeis.edu/~linearroad/)
  * [Linear Road: A Stream Data Management Benchmark](https://dl.acm.org/citation.cfm?id=1316732) (VLDB 2004)
* DEBS Grand Challange (planned)
  * [2011](http://debs2011.fzi.de/index.php/challenge)
  * [2012](http://www.csw.inf.fu-berlin.de/debs2012/grandchallenge.html)
  * [2013](http://www.orgs.ttu.edu/debs2013/index.php?goto=cfchallengedetails)
  * 2014 (no available online any more)
  * [2015](http://www.debs2015.org/call-grand-challenge.html)
