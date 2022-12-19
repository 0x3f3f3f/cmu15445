# lab1---buffer_pool

## 一：设计

+ 三层结构
  + 最底层lru结构：维护可以替换的页框号（在内存的号），不能替换的被线程占用的（需要从lru中剔除），剩下的在空闲列表中的页框号。
  + 缓冲管理池
    + 需要加锁，这里加锁以后，lru是可以不用加锁的，加了没有意义。
    + 维护一个空闲页框表，可替换的页和被线程占用的页号到页框的一个映射，再就是维护lru（可替换的页），空闲页框表和后面两个数据结构明显是互斥的。
    + 操作的时候，主要针对三个数据结构的更改，以及Pages中是否重置内存，是否更改页号，是否是脏页，是否需要操作钱读写磁盘操作。
  + 并行管理池，缓冲管理池因为加了锁，为了实现并行，需要设计多个缓冲管理池，类似组相联映射，页号%缓冲管理池的数量，自然得到在哪个池子里面，一开始生成页号在缓冲管理池给出了实现函数。



### 错误设计：

+ 设计的时候一开始采用全相联映射，新生成的页，从序号为0的缓冲管理池遍历，有空位就放进去，任意一个页号可能存在于任何一个缓冲管理池中，必须在并行管理池中维护一个哈希表，负责把相应的页号映射到对应的缓冲管理池中。每个缓冲管理池自己同样维护一个哈希表，完成页号到页框号的映射。

+ 如下所示，是并行管理池写的代码，必须要在下面加锁，假如我某个线程完成过fetch操作，也就是我已经把页读入到某个缓冲管理池中，但是这个时候，我没有更新过并行管理池中的哈希表，另外一个线程可能恰好要操作这个页面，他在并行管理池的哈希表中没有找到对应页号，就会再进行一轮循环，找可以进行fetch的缓冲管理器，可能导致重复读取，效率低下
+ 上面的问题可以加锁的方式解决，但是这个时候多个缓冲管理池并行必然造成限制，并行行大幅度下降。
+ 还有一个问题就是这个parallel并行管理池维护的哈希表会越来越大，下面的fetch操作假如找一个没有读入过的页面，这样中间层缓冲管理池那里假如完成替换以后，这个时候返回新的页面，但是之前页号并没有在并行缓冲管理池删除。

```cpp
auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  std::lock_guard<std::mutex> lock(latch_);
  if (page_id_to_instance_.count(page_id) == 0) {
    for (size_t i = 0; i < this->bpms_.size(); ++i) {
      Page *page = bpms_[i]->FetchPage(page_id); // 这里我可能刚读完磁盘，不加锁，我可能还没写入到哈希表中，导致别的线程同样需要这个页号的时候，以为没有读过，造成再一次的读磁盘操作。
      if (page != nullptr) {
        page_id_to_instance_[page_id] = i;
        return page;
      }
    }
    return nullptr;
  }
  return GetBufferPoolManager(page_id)->FetchPage(page_id);
}
```



### 正确设计：

+ 缓冲管理池中设计一个页号生成函数，如下所示，一开始每个缓冲管理池都有一个缓冲管理池号，假如有五个缓冲管理池，0号存储的页面就是(0, 5，10，15，20 。。。)。

```cpp
auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}
```

+ 好处:不用管什么映射，直接根据取余操作，不用什么哈希表就能完成映射操作，而且除了newpage之外的所有函数都变得简单了所有操作扔给中间层就可以。

```cpp
auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return bpms_[page_id % bpms_.size()];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->FetchPage(page_id);
}
```



## 二：细节点解释

+ 并行管理池必须即使换成了取余的方式，也必须加锁，原因一开始newpage操作，我肯定是按照页号依次递增的方式去读磁盘，有一个全局的变量。

```cpp
auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  std::lock_guard<std::mutex> lock(latch_);
 
  Page *page = nullptr;
  for (size_t i = 0; i < bpms_.size(); ++i)
  {
    page = bpms_[start_new_page_idx_]->NewPage(page_id);
    start_new_page_idx_  = (start_new_page_idx_ + 1) % bpms_.size();
    if (page != nullptr)
    {
      return page;
    }
  }
  return nullptr;
}
```



## 三：get

+ 编码规范（clang-tidy）
  + 函数命名最后加下划线
  + if必须写花括号
  + return后面不能有空行
  + 并行缓冲管理池删除操作遍历的时候，用auto替换



## 四：坑

+ Allocate分页号的时候，注意没有页可以分的时候，不能执行这个函数，因为会错乱。
+ 某个页的pin不能都是++操作，新new一个页需要替换的时候（或者fetch替换掉 的时候），pin一定从1开始
+ vector<A*>一定要手动对每一个元素进行释放




