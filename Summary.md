本文档主要是总结自己学习MIT6.824课程的过程。

## 时间线
学习该课程的起因有两点：第一是经过2022年秋招，认识到自己在项目经验上存在不足；第二是秋招offer的岗位方向与分布式有关。我学习的是[2022年春季课程](http://nil.csail.mit.edu/6.824/2022/schedule.html)，学习该课程的过程主要分为下面两个阶段：
* 2022.11.28～2023.2.14：完成了Lab1和Lab2，阅读了google的GFS、MapReduce和BigTable三篇论文，以及介绍Raft的论文。
* 2024.2.1～2024.2.25：完成了Lab3和Lab4。

## 课程内容
课程主要分为Lab实践与论文阅读两部分。只有前两个Lab需要熟读对应的论文才能完成，后两个只需要阅读Lab描述即可。

### Lab实践
在做具体的Lab之前，需要完成下面知识的学习：
1. Go语言：Lab使用的编程语言是go，我之前没有用过go。在做Lab之前，我看了一遍[Go语言圣经](https://gopl-zh.github.io/)，比较重要的知识点有数组、切片（原理）、map、Goroutines、Channels、基于select的多路复用、基于共享变量（锁、条件变量等）的并发等。
2. RPC和线程：了解基本的概念和原理即可。课程使用的rpc包是改写的官方的```net/rpc```，主要是为了方便模拟各种网络问题以测试lab代码的正确性。
3. Python：会使用和简单修改Python脚本即可。调试分布式程序是很困难的，课程给出了一个便于学生调试代码的方法，[Debugging by Pretty Printing](https://blog.josejg.com/debugging-pretty/)。

在所有的Lab中，学生只需要实现功能即可，不用自己编写测试用例。

#### Lab1: MapReduce
实现MapReduce论文中Coordinator（协调者、master）和Worker程序。Worker进程负责执行Map或Reduce任务；Coordinator负责分发Map和Reduce任务给Worker，以及处理执行失败的Worker进程。

难点：不熟悉Go的使用，很容易写出bug。

#### Lab2: Raft
基于[Raft论文](http://nil.csail.mit.edu/6.824/2022/papers/raft-extended.pdf)，实现Raft协议的leader选举、日志复制、持久化、日志压缩四个部分。

Raft协议：Raft是一种RSM（replicated state machine protocol）协议。复制服务（replicated service）通过在多个复制服务器上存储其状态（即数据）的完整副本来实现容错。复制允许服务继续运行，即使它的一些服务器遇到故障（崩溃、网络问题）。Raft将客户端请求组织为一个序列，称为日志，并确保所有复制服务器都看到相同的日志。每个副本根据日志以相同的顺序执行客户端请求，从而具有相同的服务状态。

难点：调试；几乎完整的实现一个raft协议。

#### Lab3: Fault-tolerant Key/Value Service
基于Lab2实现的Raft协议，实现一个容错的KV数据库。server提供三个RPC接口供client调用，Put接口用于更新或新增键值对，Get接口用于获取给定键的值，Append接口用于给键值对的值添加内容（给定键不存在时，和Put的功能一样）。

难点：确保Linearizability；确保每一个client请求只执行一次（唯一标识一个client请求），client给leader发完请求之后，leader崩溃，该请求可能不会被raft接受，也有可能被接受，client没有收到leader的响应，会尝试给新选举出的leader发同样的请求，从而可能造成请求被执行多次；确保在leader崩溃场景下服务的正确性，持久化必须的数据。

#### Lab4: Sharded Key/Value Service
在Lab3的基础上，基于Multi-Raft实现一个多分片的容错的、负载均衡的KV数据库。

难点：数据分片，将数据均衡分配给数个Lab3实现的KV数据库（比如数据分成128分片，有4个复制KV数据库，每个KV数据库分配32个分片），每个分片具体分配给哪个数据库的信息存储在Shared Controller服务中；分片迁移，在某个KV数据库丧失对某个分片的所有权后，停止服务对应的client请求，然后把该分片的数据迁移到另一个KV数据库；分片垃圾回收，分片迁移完成后，删除本地存储的该分片的KV数据；分片迁移时读写优化，多个分片迁移时，迁移完成的分片可以正常服务client请求，而不用等待所有分片迁移完成。

### 论文阅读
课程推荐阅读的论文大约有15篇，我目前只阅读了和Lab相关的5篇论文。后面计划首先看看zookeeper、spanner、Memcached at Facebook这三篇。

## 总结
做完课程的Lab，自己对课程的知识点有了更深入的认识，调试代码的能力有了很大的提升，也更有兴趣去阅读一些工业项目的源码。（个人感觉MIT6.824的Lab的难度比CMU15445要大，因为很多功能都是从0开始实现的。）