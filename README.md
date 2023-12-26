# 分布式系统

分布式系统实践+go入门（项目资料汇总）

---
## content
- cos418（2022）
- mit6.824/6.5840
- go-patterns (go相关参考)
- docs
  - [论文汇总](./docs/papers.md)

---
## 完成进度

Cos418 assignment
- [X] Mapreduce
    - [X] 1.2-序列运行
    - [X] 1.3-分布式运行
- [X] 2-chandy lambert
  - 分布式snapshot算法
- [X] raft
    - [X] 3-leader 选举
    - [X] 4-log 共识
- [ ] 5-kv store（）
  - [X] 没有完全写出来，test1只能通过部分
		
---
Mit6.824 lab
- [ ] Lab1 Mapreduce 
  - 与cos418架构不一样，不好迁移
- [X] Lab2 
    - [X] 2A  选举算法  
    - [X] 2B  log共识
        - Rejoin 不是一直能通过
        - go test -run TestRejoin2B -race
        错了两次，后续测了十几次都通过，无法复现bug了。
    - [X] 2C  persistence
    - [ ] 2D 
    
- [ ] Lab3
    - [ ] 3A kv-store
        - [X] 排查出了raft里persistence的接口问题
          - 已在lab中修正，cos618中未更改
        - [X] 尝试了另一种写法，一致性强度增加（单client一致性感知到多client一致性感知），还是会出现死锁的问题。
        - [X] 3A写不出来，还真有可能是raft写的有问题
          - 已验证，raft端使用bin依然存在现有问题
        - [ ] 再用race测一测：加个race就一直通过测试（无语）
        - [ ] ！待测试：差不多知道为什么会卡死了（写协程爬虫有感），一个线程for循环极致占用，就没法接受到其他goroutinue传来的信息，但是go routinue是什么级别的东西，线程or协程。
    - [ ] 3B
- [ ] Lab4
    - [ ] 4A
    - [ ] 4B

---
### 学习过程（个人经验）
1) Concurrent and Distributed Systems 2021-22（university of cambridge）
   - [homepage](https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/)
   - [video](https://www.youtube.com/watch?v=UEAMfLPZZhE&list=PLeKd45zvjcDFUEv_ohr_HdUFe97RItdiB&ab_channel=MartinKleppmann)
    
    Martin Kleppmann的视频课程不长，提供的[课件](https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/dist-sys-slides.pdf)多看两遍也容易上手。跳过直接进行后续课程的学习问题也不是很大。
        
   - 优点：快速对整个领域有个大致了解
   - 缺点：raft的解读不如直接看原始论文
    

2) go入门最佳实践参考[go-patterns/README.md](./go-patterns/README.md).部分资料[如:effective go]更适合边实践边参考, 乱看博客走了不少弯路。
   
   国内一些详解go源码的博客没有必要在这个阶段学习。通过google IO那两个视频对go并发使用的讲解，即可快速上手项目。
   
3) cos418
   
   相比于mit6.824全在讲论文的课程，cos418的课件更适合入门。
   课件和推荐阅读的论文很有代表性，也比较容易上手。go、rpc相关的前置知识也比mit6.824要详尽。

   虽然作业大部分是从mit6.824借鉴来的，但是前两个assignment
   mapreduce和chandy-lambert算法更容易上手。raft及后续assignment只借用了一部分，建议直接用mit6.824的lab替代。

   由于没有视频讲解，能结合论文把课件ppt看懂即可继续上手作业项目。推荐阅读的分布式论文有些年代久远，部分理论性的证明可以忽略。系统相关的论文有时候看完也并不能理解全貌，不用过度纠结。
   

4) mit6.824
    推荐阅读的论文够多够全，lab项目优秀，每年都更新。lab提供了充足的参考减少开发中的bug。
    
    课程视频（指老视频），每堂课介绍一篇分布式论文，感觉不如自行阅读。对于分布式系统不容易在宏观角度把握各个知识点（课程1、3的课件ppt更适合新人）。lab对没有背景的同学不如cos418友好，且第一个项目mapreduce脚手架过少，容易被劝退。



---

### 相关链接
- [cos418 Spring 2022](https://www.cs.princeton.edu/courses/archive/spr22/cos418/schedule.html)
  
   - [cos418-2016 assigment page](https://www.cs.princeton.edu/courses/archive/fall16/cos418/a5.html) cos418/a1|a2|a3|a4|a5.html

   - [cos418-2022 assigment page](https://github.com/cos418atPrinceton/assignments_template/tree/master)

- [mit6.824/6.5840 Spring 2023](https://pdos.csail.mit.edu/6.824/schedule.html)
  - [debug](https://blog.josejg.com/debugging-pretty/)

- [cos518 Advanced Computer Systems (Spring 2019)](https://cos518.github.io/syllabus.html) todo