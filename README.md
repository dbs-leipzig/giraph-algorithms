# giraph-algorithms

Library of graph algorithms we use with Apache Giraph. 
Documentation can be found in the specific algorithm packages. 
The licence is GPLv3, so please feel to use the algorithms in your own project.

## BTGComputation

Extracts Business Transactions Graphs (BTGs) from an integrated instance graph (IIG) 
as described in [BIIIG: Enabling Business Intelligence with Integrated Instance Graphs](http://dbs.uni-leipzig.de/de/publication/title/biiig).

## LabelPropagation

Finds communities inside networks by propagating vertex labels (communities). 
Vertices migrate to the community represented by the majority of labels sent by its neighbours.
Our implementation adds a stabilization mechanism and avoids oscillating states.
See [Wikipedia (Label propagation)(https://en.wikipedia.org/wiki/Label_Propagation_Algorithm).

## AdaptiveRepartitoning

Partitions a graph using label propagation as described in [Adaptive Partitioning of Large-Scale Dynamic Graphs](http://www.few.vu.nl/~cma330/papers/ICDCS14.pdf).
The implementation uses giraph aggregators to ensure a balanced partition load.
