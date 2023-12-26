


## Fundamentals
- [ ] [Time, Clocks and the Ordering of Events in a Distributed System](http://lamport.azurewebsites.net/pubs/pubs.html#time-clocks) 逻辑时钟

- [ ] [Practical BFT (1999)](https://pdos.csail.mit.edu/6.824/papers/castro-practicalbft.pdf) Practical Byzantine Fault Tolerance

---
## Eventual Consistency and Scaling Out
最终一致性，适合高吞吐量的应用，通过设计时间戳来解决冲突。

- [X] [Chandy-Lamport](http://lamport.azurewebsites.net/pubs/chandy.pdf) 分布式snapshot算法

- [X] [Bayou](https://www.cs.princeton.edu/courses/archive/spr22/cos418/papers/bayou.pdf) 类似于iCloud的系统，容忍web partition，保证最终一致性。

- [X] [Chord](https://www.cs.princeton.edu/courses/archive/spr22/cos418/papers/chord.pdf)
- [X] [Dynamo](https://www.cs.princeton.edu/courses/archive/spr22/cos418/papers/dynamo.pdf)



---
### Replicated State Machines

- [X] [Fault-Tolerant Virtual Machines (2010)](https://pdos.csail.mit.edu/6.824/papers/vm-ft.pdf)

  
- [X] [Paxos Made Simple](http://lamport.azurewebsites.net/pubs/pubs.html#paxos-simple) 理解上不是很困难，多slot就比较繁琐
- [X] [RAFT](https://www.cs.princeton.edu/courses/archive/spr22/cos418/papers/raft.pdf) 很好读，但是实现也没有那么简单。
  - [raft可视化](https://thesecretlivesofdata.com/raft/#intro)（简单演示流程）

---
### Strong Consistency and Scaling Out with Transactions
强一致性，保证事务性。
- [X] [COPS](https://www.cs.princeton.edu/courses/archive/spr22/cos418/papers/cops.pdf)
- [X] [Spanner (2012)](https://www.cs.princeton.edu/courses/archive/spr22/cos418/papers/spanner.pdf) 使用卫星时间保证数据系统的全球一致性，强一致性的同时支持全球事务同步。
  - 系统比较复杂，没有完全看懂


---
### 系统设计理论上限

- [ ] [CAP](https://www.cs.princeton.edu/courses/archive/spr22/cos418/papers/cap.pdf)
- [ ] [PRAM](https://www.cs.princeton.edu/research/techreps/TR-180-88)
- [ ] [SNOW](https://www.cs.princeton.edu/courses/archive/spr22/cos418/papers/snow.pdf)
- [ ] [PORT](https://www.cs.princeton.edu/courses/archive/spr22/cos418/papers/port.pdf)
- [ ] [FLP](https://www.cs.princeton.edu/courses/archive/spr22/cos418/papers/flp.pdf)


---
### 大数据系统
- [X] [MapReduce (2004)](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)

- [ ] [GFS (2003)](https://pdos.csail.mit.edu/6.824/papers/gfs.pdf)
- [ ] [Bigtable (osdi06)](https://static.googleusercontent.com/media/research.google.com/zh-CN//archive/bigtable-osdi06.pdf)
  

- [ ] [ZooKeeper (2010) ](https://pdos.csail.mit.edu/6.824/papers/zookeeper.pdf)
- [X] [Spark (2012)](https://pdos.csail.mit.edu/6.824/papers/zaharia-spark.pdf)


---
### 比特币
- [ ] [Bitcoin (2008)](https://pdos.csail.mit.edu/6.824/papers/bitcoin.pdf)


### todo待分类

- [ ] [DynamoDB (2022) ](https://pdos.csail.mit.edu/6.824/papers/atc22-dynamodb.pdf)
- [ ] []() 
- [ ] []()
- [ ] []()

