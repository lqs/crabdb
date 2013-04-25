# CrabDB

CrabDB 是一个高性能、容易使用的数据库系统。

与传统的数据库系统相比，CrabDB 能将相关的数据存储在连续的位置，大幅度降低随机读写次数，减少因机械硬盘寻道带来的性能损失。

与其它的 key-value 数据库相比，CrabDB 能支持使用逻辑表达式对每个记录进行筛选，查询出满足条件的记录。

您不需要为每种查询需求建立专门的索引，更改结构也不会影响线上服务，轻松应对各种变化的需求。

## 编译与安装

CrabDB 依赖 BerkeleyDB，MessagePack 和 Tiny C Compiler。

在 Ubuntu 12.04 及以上版本可以直接安装 BerkeleyDB 和 MessagePack 软件包：

	sudo apt-get install libdb5.1-dev libmsgpack-dev

给 Tiny C Compiler 打补丁并安装：

	wget http://download.savannah.nongnu.org/releases/tinycc/tcc-0.9.25.tar.bz2
	tar jxf tcc-0.9.25.tar.bz2
	cd tcc-0.9.25/
	patch -p1 <patches/fix-libtcc-memory-leak.patch
	./configure
	make
	sudo make install
	cd ..

然后编译和安装 CrabDB：

    make
    sudo make install

## 快速入门

目前，CrabDB 仅包含 Python 客户端。以下的示例代码均为 Python 语言。

### 连接到 CrabDB

    import pycrab
    crab = pycrab.Connection(host='127.0.0.1')

	# 这个 T 对象用于编写 Python 风格的查询表达式，请参见下面的查询示例。
    T = pycrab.Record()

### 建立桶与结构

CrabDB 中的数据被存储在多个『桶』中。每个『桶』里包含相同结构的表。每个表里包含一些记录。

每个结构里可定义多个字段，每个字段的值均为整数，大小以 bit 为单位定义。例如，记录布尔值只需用 1 bit，记录包含 8 种状态的状态值只需用 3 bits。

以下的示例基于这样一个应用场景：用户可在某个地点发布照片，并且每张照片均可设置为私密。

首先我们建立一个叫 `user_photo` 的桶，它以『用户 id』作为表的键，每个表以『照片 id』作为主键存储该用户发表的图片相关信息。

    crab.user_photo.set_fields([
	    pycrab.Field(58, 'photo_id', is_primary_key=True),  # 图片 id
	    pycrab.Field(56, 'location_id'),                    # 地点 id
        pycrab.Field(1, 'is_private', signed=True),         # 是否为私密照片
        pycrab.Field(32, 'created'),                        # 创建时间
    ])

注意：CrabDB 里的布尔类型使用有符号整数，『真』为 `-1`，『假』为 `0`。这样能够使得在布尔值上的算数运算与逻辑运算结果一样。

### 插入

    user_id = 5
    table = crab.user_photo[user_id]
    # 插入一些示例数据
	table.insert({'photo_id': 100001, 'location_id': 101, 'is_private':  0, 'created': 130000001})
    table.insert({'photo_id': 100002, 'location_id': 102, 'is_private':  0, 'created': 130000002})
    table.insert({'photo_id': 100003, 'location_id': 103, 'is_private': -1, 'created': 130000003})
    table.insert({'photo_id': 100004, 'location_id': 104, 'is_private':  0, 'created': 130000004})
    table.insert({'photo_id': 100005, 'location_id': 105, 'is_private':  0, 'created': 130000005})
    table.insert({'photo_id': 100006, 'location_id': 101, 'is_private': -1, 'created': 130000006})
    table.insert({'photo_id': 100007, 'location_id': 102, 'is_private':  0, 'created': 130000007})
    table.insert({'photo_id': 100008, 'location_id': 103, 'is_private':  0, 'created': 130000008})

### 查询

CrabDB 会将查询条件编译后再运行，以提高全表扫描的性能。

    # 查询最近的 10 条记录
    for r in crab.user_photo_test[user_id].find().sort(-T.created).limit(10):
	    print r

    # 查询由 user_id 发布的在 [101, 102, 103, 104] 这四个地点的非私密照片，按时间倒序排序
    location_ids = [101, 102, 103, 104]
    table = crab.user_photo_test[user_id]
    query = table.find(T.location_id.in_(location_ids) & ~T.is_private)
    for r in query.sort(-T.created):
	    print r

    # 查询不同的地点
    for r in crab.user_photo_test[user_id].find().group(T.location_id):
        print r

### 更新
	
	# 将 photo_id 为 100005 的照片设置为私密
    crab.user_photo_test[user_id].find({'photo_id': 100005}).update({'is_private': -1})

### 删除
    crab.user_photo_test[user_id].remove({'photo_id': 100004})

