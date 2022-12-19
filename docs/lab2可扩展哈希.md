# lab2可扩展哈希

## 复习解释：

### directory

+ 维护每个bucket的local_depth和page_id，以及目录页的page_id,和一个global_depth
+ 传入key，有一个hash函数自动计算出一个idx，对应于directory存储的不同的页。操作某个页的时候，利用lab1的缓冲管理获取对应的页。



几个重要的函数讲解：

+ insert导致split_insert操作的时候，需要找到分裂页的位置。

```cpp

auto HashTableDirectoryPage::GetLocalHighBit(uint32_t bucket_idx) -> uint32_t {
  // assert(bucket_idx < Size());
  return (1 << (local_depths_[bucket_idx] - 1));
}
// 例如：下标1，3指向的是同一个页，插入过程导致1满了，这个时候要分裂，3就是split_page，但是要计算这个页的下标，因为要产生3新对应的页，必须得到idx，这个时候idx为1的页对应二进制是1,1 ^ 10得到的就是split_idx，这两个函数就是得到idx对应要异或的对象10的用的。
auto HashTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) -> uint32_t {
  // assert(bucket_idx < Size());
  return bucket_idx ^ GetLocalHighBit(bucket_idx);
}
```

+ 插入分裂的时候，会引发全局global_depth的增加。

```cpp
void HashTableDirectoryPage::IncrGlobalDepth() {
  uint32_t si = Size();
  assert(si < DIRECTORY_ARRAY_SIZE);
  for (uint32_t old_st = 0, new_st = si; old_st < si; ++old_st, ++new_st) {
    this->local_depths_[new_st] = this->local_depths_[old_st];
    this->bucket_page_ids_[new_st] = this->bucket_page_ids_[old_st];
  }
  this->global_depth_++;
}
```

![image-20221007213155241](https://zhanghao1004.oss-cn-hangzhou.aliyuncs.com/image-20221007213155241.png)

+ 上面函数全局深度增加1，相当于directory的槽翻一倍，然后0，2指向同一个页，翻译在实现上就是idx为0和2对应的local_depth和page_id是相等的。



+ 全局子网掩码和局部子网掩码

```cpp
// 就是专门用来 hash(key) & GetGlobalDepthMask（）
auto GetGlobalDepthMask() -> uint32_t;
// 这里是插入导致的分裂用的，分裂的话，假如1和3原来指向相同的页，假如key对应的idx是1号位置，即3要生成一个全新的空数据的页，然后把hash(key) & GetLocalDepthMask()把1号页的所有数据重新分到这两个页中,用GetGlobalDepthMask也是可以的。
auto GetLocalDepthMask(uint32_t bucket_idx) -> uint32_t;
```





### bucket类

+ occupied表示是否被占用过，readable表示是否当前key和value是存在的，用char数组，char数组大小都是固定的，提前根据key和value计算好的，这个类不能随意插入别的数据，测试会出错。
+ 其余部分全存储key和value，可扩展哈希允许存储相同的key，但是不允许存储key和value都相等的情况。
+ 采用的是位示图法，一定注意会有多余的位的情况，大概率不会恰好相等，因此计算是否满，不能遍历所有的readable_的每一位。要遍历的是这个页能装的key和value的最大值。



### extendible类

辅助函数（incline），帮助下面五个函数（get_value,insert,split_insert,remove,merge）的辅助函数，例如根据key获得idx，根据key获得page_id,根据page_id获得对应的页等等。

一个获取值的函数

插入（加分裂）：

1. 直接插入（一开始directory和创建的第一个bucket local_depth和global_depth都是0）
2. 插入失败可能是已经存在key和value都相等的情况，这个时候就返回插入错误
3. 另外插入失败，是因为bucket的页满了，这个时候分裂
   + 如果directory槽的个数已经达到最大值，直接返回false，因为不能继续增加global_depth。
   + 对应idx的local_depth<global_depth直接增加local_depth，否则需要先增加global_depth
   + 把指向相同页的槽的local_depth都要改成相同的local_depth，计算distance = 1 << directory->GetLocalDepth(directory_idx)  注意使用的是增加以后的local_depth。
   + 向前，向后都要改，而且split_idx也要修改
   + 创建split_idx创建新的页面，然后把原来的数据根据GetLocalDepthMask/GetGlobalDepthMask计算，重新分到这两个页面中。
   + 重新递归insert操作，注意很有可能出现多个key都相等，一直在一个bucket中， 一直分裂的情况。

删除：

1. 直接删除
2. 不管删除成功还是删除失败，都要判断page里面的数据是否为空
3. 只要为空就要进行merge操作
   + split_idx的local_depth一定要和目标idx的local_depth相等，否则返回
   + split_idx的local_depth不能为0，否则返回
   + 遍历directory所有和当前idx和split_idx的page_id相等的槽，把他们都local_depth都变成相等的，因为目标页已经相当于被删除了。
   + 然后检查directory是否可以shrink操作



## 面试点

### 锁

+ 一共相当于使用了三种锁，一个mutex，一个读写锁，多个page自带的锁（实现颗粒度锁的概念）
+ 首先get_value和insert和remove三个函数调用的时候都是加读锁，读锁本质上加在directory，只要对directory不进行修改就可以只加上读锁，而且读锁本质上是一种可重入锁，因为insert的时候可能发生多次递归调用。
+ 写锁加在split_insert和merge操作上，因为这两个需要对directory进行修改，修改对应的local_depth和page_id。
+ 也就说在insert进入split_insert之前必须释放读锁，否则死锁，然后split_insert的调用insert的时候及时释放写锁。remove那边同理。
+ 所有修改具体page数据的时候，都是加page自带的读写锁，这个又是进一步提高并发性的，例如插入删除肯定对page修改，就单独修改的加上写锁，然后不修改的地方加上读锁，这个时候，针对某个具体的page加锁减小了锁的颗粒度，同时避免错误的发生。
+ 如果只是单纯的加可重入锁相当于是写锁，insert和remove或者get_value不能同时进行。

