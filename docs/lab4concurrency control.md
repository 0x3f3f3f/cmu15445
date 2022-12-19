# lab4:concurrency control

## 一：基本知识

X锁和S锁和平常多线程的锁不是一回事，X和S锁针对事务隔离性，保证并发性同时不产生错误的。

### 悲观锁保证的隔离级别:

|      隔离级别       |                          防止的问题                          |
| :-----------------: | :----------------------------------------------------------: |
|    1.最低的级别     | 所有隔离界别必须遵守防止脏写，脏写就是所有隔离界别写完都得commit之后释放，写锁。（这个要求已经默认所有的隔离级别必须都要遵守严格的2PL） |
|  2.read uncommited  | 因为脏写的存在，读未提交隔离级别，不能加S锁，加了因为X锁肯定commit以后释放，无法出现脏写。 |
|   3.read commited   | 这种隔离界别，读要加上读锁，读锁是不用满足2PL的，写锁必须满足严格2PL协议规则，读锁释放在commit之前，会出现不可重复读的问题。 |
| 4.RR(repeated read) | RR的时候必须满足严格的2PL，读可以commit之前释放，读锁满足普通2PL，写锁满足严格2PL，这样就能保证没有脏读和保证可重复读 |
|      5.序列化       |            直接索引加上锁，索引加锁防止幻的情况。            |

1. t1写r1，t2写r1，导致脏写，因为有一个事务写的东西被覆盖了
2. t1写了r1，t2读的时候没有S锁就可以读，就会读到没有提交的事务，t1abort就导致脏读
3. t1两次读，没有2PL的加持，就会导致t2，在两次读之间加了写操作，导致两次读的内容不一致，读未提交：读锁自由释放的，不用满足任何关于2PL的东西，写锁必须满足严格2PL。
4. 读锁至少满足普通的2PL，可以commit之前释放，但是写锁需要施行严格的2PL，这样才能避免脏读和可重复读。
5. 以上都是针对已经有的数据，对于插入操作，由于对索引没有加锁，很有可能导致幻读。



### 隔离级别还可以乐观锁保证

+ MVCC（timestamp--时间戳的一种）

### sql语句

+ mysql中的select语句没有任何锁
+ select * from table for update;自动加上写锁
+ select * from table lock in shared mode；自动加上读锁
+ 没有后面的修饰不加任何锁。
+ insert update delete操作都是自动加上X锁

### unrecoverable schedule and cascadeless schedule（无级联）

![image-20221219143608787](https://zhanghao1004.oss-cn-hangzhou.aliyuncs.com/image-20221219143608787.png)

+ 事务T2commit以后成为不可恢复



![image-20221219143726880](https://zhanghao1004.oss-cn-hangzhou.aliyuncs.com/image-20221219143726880.png)

+ T2和T3并没有提交，T1abort就可以让另外俩也undo操作，但是这样会导致做的工作白费。
+ 排他锁commit之后释放可以保证无级联

+ 总结：unrecoverable schedule一定属于cascadeless schedule



### 2PL协议

| 类型      | 说明                                                         |
| --------- | ------------------------------------------------------------ |
| 普通的2PL | 读锁和写锁都要满足growing阶段加锁，shrinking阶段释放锁       |
| 严格的2PL | 写锁必须满足commit或者abort阶段进行锁的释放，读锁至少满足普通的2PL条件，即commit之前可以释放读锁，但是满足growing阶段加锁，shrinking阶段解锁 |
| 强2PL     | 读写锁的释放都得在commit之后                                 |



### 死锁措施

#### 死锁预防（Lock_manager）：

+ wound-wait，事务id越小，代表事务的优先级越高，一个记录的加锁事务等待队列中从前往王进行遍历，首先前提是碰到读写，写写，读更新这三种互斥的前提，遇到id比当前事务更大的，直接abort，只要有一个id比自己还小的，当前事务就要进行睡眠，等待前面优先级更大的执行完成，等前面全部执行完成，自己还要每次循环检查的时候，还要检查自己有没有被abort。
+ 被abort的事务的id不能随着restart的时候更新，因为可能导致有些事务一直无法执行。
+ 等确定自己可以拿到锁以后再把对应事务的can_grant赋值为true

#### 死锁避免

+ DFS或者拓扑排序判断

### 锁升级的实际好处

![image-20221219174204886](https://zhanghao1004.oss-cn-hangzhou.aliyuncs.com/image-20221219174204886.png)

没有更新锁，t9根本没法同时进行读操作，完全就是用来提高并发性的。



### 条件变量

+ notify_all的时候，存在等待当前锁的等待队列，从前往后依次唤醒等待的事务，例如，事务1，唤醒，事务2，3，4等待，唤醒操作以后，事务2，3，4会一次等待被唤醒，程序验证过。
+ 每当检测当前记录的事务等待队列中有被abort的事务，就要执行，让这些事务退出队列。
+ 某个事务unlock也要执行唤醒别的事务继续进行。





### U锁的好处

https://blog.csdn.net/samjustin1/article/details/52210125#reply

这里的U锁不是锁升级，单纯是更新锁的作用。但是都是为了解决写锁并发度卡的太死的问题。

### 多粒度锁

### MVCC



## 二：好的资源（面试）

书籍：数据库系统概念

讲解unique_lock和shared_lock如何实现读写锁的https://blog.csdn.net/qq_33726635/article/details/109693403

[2021 CMU 15-445 实验笔记 – 沧海月明](https://www.inlighting.org/archives/cmu-15-445-notes)

讲解等待图，解决死锁的一种方法，以及介绍了U锁等内容，2020级CMU：https://www.cnblogs.com/JayL-zxl/p/14613562.html



