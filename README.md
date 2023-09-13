# 分布式系统

### 课程项目
- cos418（2022）



完成进度

Cos418 assignment
- [X] Mapreduce
    - [X] 1.2（序列运行）
    - [X] 1.3（分布式运行）
- [X] 2、chandy-lambert（分布式snap-shot算法）
- [] raft
    - [X] 3 选举
    - [X] 4 共识
		5_half（没有完全写出来）
		
Mit6.824 lab

- [] Lab1 Mapreduce （不写了，架构不一样，不太好整）
- [] Lab2 
    -[] 2A  选举算法
        补充测试 -race
    - [] 2B  log
        测试通过
        补充测试 -race
        Rejoin 不是一直能通过
        go test -run TestRejoin2B -race
        错了两次，后续测了十几次都通过，无法复现bug了。
    - [] 2C  persistence
        测试通过
        补充测试 -race
    - [] 2D
    
- [] Lab3
    - [] 3A kv-store
        尝试了另一种写法，还是会出现死锁的问题。
        再用race测一测：加个race就一直通过测试（无语）
        3A写不出来，还真有可能是raft写的有问题
    - [] 3B
- [] Lab4
    - [] 4A
    - [] 4B