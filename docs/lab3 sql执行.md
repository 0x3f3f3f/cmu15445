# lab3 sql执行

## select

SELECT col_a, col_b FROM test_1 WHERE col_a < 500

+ table_heap_保存着遍历一个表最原始的tuple的迭代器
+ 根据迭代器可以得到tuple以及RID(一个tuple的identifier)，然后根据输出格式，得到输出的列，再根据列中的getExpr()函数得到输出列的Value，这样就得到目标tuole的多个Value
+ 利用vector<Value>构造得到新的tuple
+ plan_中有predicate（谓词），简单点就是col_a < 500，因为选出的新的tuple需要进行过滤，predicate == nullptr就代表没有where的限制条件
+ 一直到迭代器到达end()



### 注意点：

+ 把一个unique_ptr封装的赋值的时候，因为不能两个指针指向同一个区域，切忌把原来的unique_ptr通过move或者release()方法释放，因为很有可能导致上层的类再使用的时候出现错误，比如这里的table_heap_，除非上层传递的就是一个右值的变量，unique_ptr<A> &&a,这种情况下是可以move转移。
+ 使用reserve配合emplace_back，相比resize和push_back好处，resize直接创建默认的对象，reserve单纯开辟空间，emplace_back可以原地构造，相比push_back的好处如下：

```cpp
class A{
public:
	int a_;
	A(int a){};
}

vector<A> arr;
arr.emplace_back(3);
arr.push_back(A(3));

// cpp11以后这两个都已经支持右值，但是push_back的局限是必须传递的是对象，不能像emplace_back那样传递
```



## Insert

INSERT INTO empty_table2 VALUES (100, 10), (101, 11), (102, 12)

INSERT INTO empty_table2 SELECT col_a, col_b FROM test_1 WHERE col_a < 500

+ 插入在plan中会多出一个IsRawInsert的函数判断是否是直接插入，还是有子执行器的执行，就算有执行器也最多只有一个，但是这个执行器可以继续嵌套，第二个例子来说，我执行子执行器得到就是一个筛选结束的，不管你嵌套多少。
+ 是raw类型直接vector<Value> arr直接利用输出schema获得所有的column，插入的数据也是plan_中给到的，直接构造tuple，多次插入，把多条数据利用table_heap插入，同时插入需要更新索引，所以在catalog(表目录)通过getTableIndexes获得，Index类也是有对应的插入索引的API，注意传入的key不是直接传入tuple，需要转换KeyFromTuple函数转成对应的key tuple
+ 注意一个表索引多个，所以是要更新所有的索引，对每一个Index类都要执行一边InsertEntry。
+ 如果不是raw数据，而是上面第二个例子那样，首先直接executor执行Init()，然后调用Next获得一条tuple，因为这个时候相当于是构造新的表的形式，或者说select选出的schema和目标表schema一致，直接调用写的插入数据和索引的函数，服用raw数据插入函数即可。
+ 注意Next插入成功必须返回false，因为executor执行的时候都是统一while循环的方式，一条条的获得Next的执行结果，不返回false，上层会一致进行插入操作。具体看实现函数。

### 注意点：

+ 形参写引用，要用const修饰，这样会导致const对象不能调用非const对象，要注意，如果是要修改内容，就用指针，这是编码规范
+ 注意动态绑定必须释放内存。最笨的就是直接 A a的方式，静态分配。
+ 头文件顺序问题，c库文件，cpp库文件，其他头文件，自己项目的头文件



## Update

UPDATE test_3 SET colB = colB + 1;

+ 必然有一个子执行器需要执行，首先获得原始数据，然后直接调用更新函数
+ 修改数据的函数已经完成，直接传入老数据，生成的新数据，直接用table_heap更新，所引同样是和insert一样对所有索引进行修改，采用先删除后添加的方式更新

## Delete

DELETE FROM test_1 WHERE col_a == 50;

+ 删除根据rid进行删除的，所以子执行器直接获得对应的tuple和rid
+ 删除采用标记删除的方式，数据先markdelete，不是真正的删除，所以直接删除即可



## nested_loop_join

选出符合条件的tuple，注意是经过映射以后的

SELECT test_1.col_a, test_1.col_b, test_2.col1, test_2.col3 FROM test_1 JOIN test_2 ON test_1.col_a = test_2.col1;

+ 因为基于火山模型，Next的方式一条一条获得，相当于做的完全交叉检验，Next执行获取tuple不显示，需要在Init（）获得所有的结果，然后Next通过下标一个一个的返回
+ 两个执行器执行，每次Next获得一个新的tuple，注意内层的executor需要每一轮循环重置一次Init（），每次获得两个tuple必须通过predicate进行判断，是否符合内连接的条件，最终返回。很明显效率低下，生成的结果也是根据out_put_Schema生成的，生成的rid是不存在的，通过内连接的rid不存在，因为是临时的。

### 注意事项：

+ 

RID a, b;

RID a;

RID b

分开写，作为代码规范

+ 全局的idx必须初始化的时候主动赋值为0，测试的就出现过错误。



## hash join

SELECT test_1.col_a, test_1.col_b, test_2.col1, test_2.col3 FROM test_1 JOIN test_2 ON test_1.col_a = test_2.col1;

完全相当于对Inner join的优化，首先把一个表放到哈希表中，注意一个槽可能多个tuple存在，这样第二个表计算的key相同的tuple需要一次和这些tuple组合成结果。

+ 对于第一个遍历的表，首先获得初始的tuple和对应的schema，然后传入plan中的LeftJoinKeyExpression，相当于找到key对应的Value值，放到一个结构体中的Value成员，因为哈希函数写的时候就是对这个结构体封装的，所以key相当于是结构体HashJoinKey类型，相同key放到一个vector中。

+ 第二表的遍历，通过RightJoinKeyExpression找到对比关键字的Value，同样初始化一个HashJoinKey类型的变量，然后去找哈希表，找到，直接根据GetSchema计算新的tuple，用的也是EvaluateJoin函数，因为需要从两个tuple中提取目的tuple。
+ Next写法和上面一样

### 优化

+ 完全就是空间换时间的方式，优化时间复杂度



## aggregate 

SELECT count(col_a), col_b, sum(col_c) FROM test_1 Group By col_b HAVING count(col_a) > 100

聚合函数，注意结果不止一个、having，相当于predicate

+ 题目已经给了哈希类（包含哈希表），哈希表的迭代器
+ 同样执行子执行器得到最原始的数据，把原始数据tuple传递给哈希类，通过InsertCombine函数，找到目标列的值，用新的tuple对哈希表中目标列值，count,sum,min,max等记录的值用新的tuple进行一个更新。group by就是使用哈希值的原因，会找到对应哈希值的tuple（和传入的tuple完全不是一回事，是记录count，min，sum等的一个tuple），对他的count,sum,min,max的值，用新的tuple进行更新。
+ 哈希表完成上面操作以后，对迭代器进行Begin操作
+ 每一次Next通过迭代器获得Key和Value，因为通过outputschema计算的时候需要用EvaluateAggreagate，需要用到key和value，得到对应的Value，组成新的tuple
+ 同样和seq_scan一样，当迭代器到达End()的时候，返回false
+ 注意如果不满足having条件的时候，return的时候要继续递归Next()

## Distinct

+ 提取数据直接通过子执行器的得到tuple，然后输出和获得tuple的格式相同的直接通过tuple的getValue计算Value，然后组成vector<Value>,用来赋值给DistinctKey里面的vector<Value>成员，原理和hash join一样，都是用来哈希表使用的，因为distinct是根据所有列判断相同不同，等构造出DistictKey以后，判断哈希表中有无，决定是否返回。



## Limit

略









