### 通用jdbc高并写入（新）

一个新版本的jdbc写入服务。  
基于`JDK17`及以上版本。

- 可自定义jdbc驱动
- 可自定义监听主题
- 可自定义写入语句拼装规则
- 易修改的拼装规则
- 兼容jdbc和kafka
- 易修改的资源和配置
- 简易web页面

### 使用方法

修改配置文件，启动项目，访问`http://localhost:8080`即可。  
项目同目录下需要有目录`conf`、`parse`、`driver`，目录内的副本在`provided`下，拷贝使用即可。

#### 配置文件

##### 1. server.properties

```properties
mqtt=true
kafka=true
```

使用mqtt或kafka,需要修改为true，否则为false。
并在`application.yml`中添加或删除对应的配置。

##### 2. driver.properties

```properties
com.mysql.cj.jdbc.Driver=mysql-connector-j-8.2.0.jar
#com.clickhouse.jdbc.ClickHouseDriver=clickhouse-jdbc-0.6.0.jar
```

添加自定义的jdbc驱动，格式为`驱动类名=驱动jar包名`。  
需要在`driver`目录下添加对应的jar包。  
因为该项目仅加载一个驱动，所以jar包有一个即可。有多个数据库有写入需求的话，可以启动多个项目。  
在`provided`目录中提供了相应的jar包，可拷贝使用。

##### 3. parse.properties

```properties
com.valhalla.holder.base.parse.JsonStructMessageParse2Sql=generic-jdbc-writer-parse-json-array-structural.jar
```

类似驱动，这是解析规则的配置。  
可以添加多个解析规则，格式为`解析类名=解析jar包名`。第一个加载的解析规则为默认规则。  
需要在`parse`目录下添加对应的jar包。
来自模块`generic-jdbc-writer-parse-json-array-structural`的解析规则，可以解析json数组，且json数组中的每个json对象的字段顺序相同。

###### 如何添加自定义解析规则

1. 复制`generic-jdbc-writer-parse-json-array-structural`模块，修改模块名。
2. 修改`pom.xml`中的`artifactId`和`name`。
3. 修改`MessageParse2Sql`实现类，实现自己的解析规则。
4. 打包，将jar包放入`parse`目录下。

#### 4. mqtt.properties 和 kafka.properties

这两个配置文件中的是订阅主题和映射数据表的规则。  
参照这两个配置文件中的规则，可以直接修改。  
在页面或api中reload配置后，会重新加载配置文件中的规则。  
也可以通过页面或api修改规则，修改后会自动保存到配置文件中。

### 如何打包

将需要的jar包放置于相应的项目目录下。  
打包`generic-jdbc-writer-dist`即可，将得到项目zip文件，jar包和配置目录将一并按需求打入zip文件。

#### Q&A

### 为什么使用`spring-boot-starter-jdbc`而不是`spring-boot-starter-data-jdbc`？

1. `spring-boot-starter-jdbc`：这个 starter 提供了基本的 JDBC 支持，包括 `DataSource` 和 `JdbcTemplate` 的自动配置。它适用于你需要直接使用
   SQL 语句进行数据库操作的场景。
2. `spring-boot-starter-data-jdbc`：这个 starter 是 Spring Data JDBC 的入口，它在 `spring-boot-starter-jdbc`
   的基础上，提供了一个更高级别的抽象，使得你可以使用更简洁的代码进行数据库操作。Spring Data JDBC
   提供了包括实体持久化、查询方法自动实现等功能，它适用于你希望使用类似于 JPA 那样的数据访问层框架，但又不需要 JPA
   那么复杂功能的场景。

- 总的来说，你应该根据你的具体需求来选择使用哪个 starter。如果你需要直接使用 SQL
  进行数据库操作，那么 `spring-boot-starter-jdbc`
  可能是更好的选择。如果你希望有一个更高级别的抽象，以简化数据库操作的代码，那么 `spring-boot-starter-data-jdbc` 可能更适合你。

### 关于`clickhouse-jdbc`的版本

这里使用的是`clickhouse-jdbc-0.4.6.jar`，而不是新版本。截至0.6.0版本，clickhouse-jdbc的驱动包中引用的新版本`httpclient5`
为被集成到`clickhouse-jdbc`中，而是需要单独引入。  
如果使用新版本的`clickhouse-jdbc`，需要在`server`的`pom.xml`中添加`httpclient5`的依赖。

```xml
    <dependency>
        <groupId>org.apache.httpcomponents.client5</groupId>
        <artifactId>httpclient5</artifactId>
        <version>5.3.1</version>
    </dependency>
```

### 关于ClassLoader

```
// 上下文
ClassLoader parent = Thread.currentThread().getContextClassLoader();
// 覆盖
Thread.currentThread().setContextClassLoader(urlClassLoader);
```