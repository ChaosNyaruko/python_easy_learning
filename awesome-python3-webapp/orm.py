import asyncio
import aiomysql
import logging; logging.basicConfig(level=logging.INFO)

#查询字段计数：替换成sql识别的'？'
#根据输入的字段生成占位符列表
def create_args_string(num):
    L = []
    for i in range(num):
        L.append('?')
    #用，将占位符？拼接起来
    return (','.join(L))


# @asyncio.coroutine
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )

#协程：销毁所有的数据库连接池
async def destory_pool():
    global  __pool
    if __pool is not None:
        __pool.close()
        await __pool.wait_closed()

async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with (await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs

async def execute(sql, args):
    log(sql)
    with (await __pool) as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        return affected


class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)



class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

#Boolean不能做主键
class BooleanField(Field):
    def __init__(self,name=None,default=None):
        super().__init__(name,'Boolean',False,default)


class IntegerField(Field):
    def __init__(self,name=None,primary_key=False,default=0):
        super().__init__(name,'int',primary_key,default)

class FloatField(Field):
    def __init__(self,name=None,primary_key=False,default=0.0):
        super().__init__(name,'float',primary_key,default)

class TextField(Field):
    def __init__(self,name=None,default=None):
        super().__init__(name,'text',False,default)

#定义Model的metaclass元类
#所有的元类都继承自type
#ModelMetaclass元类定义了所有Model基类（继承ModelMetaclass）的子类实现的操作

# -*-ModelMetaclass：为一个数据库表映射成一个封装的类做准备
# 读取具体子类(eg：user)的映射信息
#创造类的时候，排除对Model类的修改
#在当前类中查找所有的类属性(attrs),如果找到Field属性，就保存在__mappings__的dict里，
#同时从类属性中删除Field（防止实例属性覆盖类的同名属性）
#__table__保存数据库表名

class ModelMetaclass(type):
    #__new__控制__init__的执行，所以在其执行之前
    #cls：代表要__init__的类，此参数在实例化时由python解释器自动提供（eg：下文的User、Model)
    #bases:代表继承父类的集合
    #attrs:类的方法集合
    def __new__(cls,name,bases,attrs):
        #排除对Model的修改
        if name == 'Model':
            return type.__new__(cls,name,bases,attrs)

        #获取table名字
        tableName = attrs.get('__table__',None) or name
        logging.info('found model:%s (table:%s)'%(name,tableName))

        #获取Field和主键名
        mappings = dict()
        fields = [] #保存非主键的属性名
        primaryKey = None
        #k:类的属性(字段名)；v：数据库表中对应的Field属性
        for k,v in attrs.items():
            #判断是否是Field属性
            if isinstance(v,Field):
                logging.info('found mapping %s===>%s' %(k,v))
                #保存在mappings
                mappings[k] = v
                if v.primary_key:
                    logging.info('found primary key %s'%k)
                    #主键只有一个，不能多次赋值
                    if primaryKey:
                        raise RuntimeError('duplicate primary key for the field:%s'%k)
                    #否则设为主键
                    primaryKey = k
                else:
                    #非主键，一律放在fields
                    fields.append(k)
        #end for
        if not primaryKey:
            raise RuntimeError('primary key is not found')
        #从类属性中删除Field属性
        for k in mappings.keys():
            attrs.pop(k)

        #保存非主键属性为字符串列表形式
        #将非主键属性变成`id`,`name`这种形式（带反引号）
        #repr函数和反引号：取得对象的规范字符串表示
        escaped_fields = list(map(lambda f:'`%s`' %f,fields))
        #保存属性和列的映射关系
        attrs['__mappings__'] = mappings
        #保存表名
        attrs['__table__'] = tableName
        #保存主键属性名
        attrs['__primary_key__'] = primaryKey
        #保存主键外的属性名
        attrs['__fields__'] = fields
        #构造默认的增删改查语句
        attrs['__select__'] = 'select `%s`,%s from `%s` '%(primaryKey,','.join(escaped_fields),tableName)
        attrs['__insert__'] = 'insert into `%s` (%s,`%s`) values (%s)' %(tableName,','.join(escaped_fields),primaryKey,create_args_string(len(escaped_fields)+1))
        attrs['__update__'] = 'update `%s` set %s where `%s` = ?' %(tableName,','.join(map(lambda f:'`%s` = ?' %(mappings.get(f).name or f),fields)),primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s` = ?' %(tableName,primaryKey)
        return type.__new__(cls,name,bases,attrs)

#定义ORM所有映射的基类：Model
#Model类的任意子类可以映射一个数据库表
#Model类可以看做是对所有数据库表操作的基本定义的映射
#基于字典查询形式
#Model从dict继承，拥有字典的所有功能，同时实现特殊方法__getattr__和__setattr__,能够实现属性操作
#实现数据库操作的所有方法，定义为class方法，所有继承自Model都具有数据库操作方法

class Model(dict,metaclass=ModelMetaclass):
    def __init__(self,**kw):
        super(Model,self).__init__(**kw)
    def __getattr__(self,key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError("'model' object has no attribution:%s"%key)
    def __setattr__(slef,key,value):
        self[key]=value
    def getValue(self,key):
        #内建函数getattr会自动处理
        return getattr(self,key,None)

    def getValueOrDefault(self,key):
        value=getattr(self,key,None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.info('using default value for %s : %s'%(key,str(value)))
                setattr(self,key,value)
        return value

    @classmethod
    #申明是类方法：有类变量cls传入，cls可以做一些相关的处理
    #有子类继承时，调用该方法，传入的类变量cls是子类，而非父类
    @asyncio.coroutine
    def find_all(cls,where=None,args=None,**kw):
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy',None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit',None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit,int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit,tuple) and len(limit) == 2:
                sql.append('?,?')
                args.append(limit)
            else:
                raise ValueError('invalid limit value:%s'%str(limit))
        #返回的rs是一个元素是tuple的list
        rs = yield from select(' '.join(sql),args)
        return [cls(**r) for r in rs]
        #**r 是关键字参数，构成了一个cls类的列表，其实就是每一条记录对应的类实例

    @classmethod
    @asyncio.coroutine
    def findNumber(cls,selectField,where=None,args=None):
        '''find number by select and where'''
        sql = ['select %s __num__ from `%s`'%(selectField,cls.__table__)]
        if where:
            sql.append('where')
            args.append(where)
        rs = yield from select(' '.join(sql),args,1)
        if len(rs) == 0:
            return None
        return rs[0]['__num__']

    @classmethod
    @asyncio.coroutine
    def find(cls,primaryKey):
        '''find object by primary key'''
        #rs是一个list，里面是一个dict
        rs = yield from select('%s where `%s`=?'%(cls.__select__,cls.__primary_key__),[primaryKey],1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])
        #返回一条记录，以dict的形式返回，因为cls的父类继承了dict类

    #根据条件查找
    @classmethod
    @asyncio.coroutine
    def findAll(cls,**kw):
        rs = []
        if len(kw) == 0:
            rs = yield from select(cls.__select__,None)
        else:
            args = []
            values = []
            for k,v in kw.items():
                args.append('%s = ?' %k)
                values.append(v)
            print('%s where %s' % (cls.__select__,' and '.join(args)),values)
            rs = yield from select('%s where %s' % (cls.__select__,' and '.join(args)),values)
        return rs

    @asyncio.coroutine
    def save(self):
        args = list(map(self.getValueOrDefault,self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__insert__,args)
        if rows != 1:
            logging.info('failed to insert record:affected rows:%s'%rows)

    @asyncio.coroutine
    def update(self):
        args = list(map(self.getValue,self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = yield from execute(self.__update__,args)
        if rows != 1:
            logging.info('failed to update record:affected rows:%s'%rows)

    @asyncio.coroutine
    def delete(self):
        args = [self.getValue(self.__primary_key__)]
        rows = yield from execute(self.__delete__,args)
        if rows != 1:
            logging.info('failed to delete by primary key:affected rows:%s'%rows)

if __name__ == '__main__':
    class User(Model):
        #定义类的属性到列的映射：
        id = IntegerField('id',primary_key = True)
        name = StringField('name')
        email = StringField('email')
        password = StringField('password')
    #创建异步事件的句柄
    loop = asyncio.get_event_loop()

    #创建实例
    @asyncio.coroutine
    def test():
        yield from create_pool(loop=loop,host='localhost',port=3306,user='root',password='',db='test')#单引号表示空格
        user = User(id = random.randint(5,100),name='xh',email='xh@pthon.com',password='123456')
        yield from user.save() #插入一条记录：测试insert
        print(user)
        #这里可以使用User.findAll()是因为：用@classmethod修饰了Model类里面的findAll()
        #一般来说，要使用某个类的方法，需要先实例化一个对象再调用方法
        #而使用@staticmethod或@classmethod，就可以不需要实例化，直接类名.方法名()来调用
        r = yield from User.findAll(name='xh') #查询所有记录：测试按条件查询
        print(r)
        user1 = User(id = 2,name='xiong',email='xh@qq.com',password='123456') #user1是数据库中id已经存在的一行的新数据
        u = yield from user1.update() #测试update,传入User实例对象的新数据
        print(user1)
        d = yield from user.delete() #测试delete
        print(d)
        s = yield from User.find(1) #测试find by primary key
        print(s)
        yield from destory_pool() #关闭数据库连接池

    loop.run_until_complete(test())
    loop.close()
    if loop.is_closed():
        sys.exit(0)
