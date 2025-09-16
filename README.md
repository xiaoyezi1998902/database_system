# 项目功能说明

1. 提供一个数据库前端网页，支持显示运行结果，tokens，ast，plan，错误，缓存等信息，支持查看数据库中已存在的表的内容。

   ![image-20250915234628529](C:\Users\74187\AppData\Roaming\Typora\typora-user-images\image-20250915234628529.png)

2. 建表功能（CREATE）："CREATE TABLE student(id INT, name TEXT);"。

   ![image-20250914211558979](C:\Users\74187\AppData\Roaming\Typora\typora-user-images\image-20250914211558979.png)

   错误情况：表已存在，列名重复，不支持的类型。

3. 插入记录（INSERT）："INSERT INTO student(name, id) VALUES ('Alice', 1);"，"INSERT INTO student VALUES (2, 'Bob');"。

   ![image-20250914232939950](C:\Users\74187\AppData\Roaming\Typora\typora-user-images\image-20250914232939950.png)

   错误情况：表不存在，列不存在，INSERT 值个数不一致: 期望 {expected_count}, 实际 {len(stmt.values)}，类型不匹配。

4. 搜索功能（SELECT）："SELECT * FROM students;", "SELECT id,name FROM students WHERE id = 3 AND age >= 20;"。

   ![image-20250915161407315](C:\Users\74187\AppData\Roaming\Typora\typora-user-images\image-20250915161407315.png)

   错误情况：表不存在，列不存在，类型不匹配。

5. 删除功能（DELETE）："DELETE FROM students WHERE id = 2 AND age > 18;", "DELETE FROM student;"。

   ![image-20250915195350502](C:\Users\74187\AppData\Roaming\Typora\typora-user-images\image-20250915195350502.png)

   错误情况：表不存在，列不存在。

6. 缓存命中分析功能，支持输出替换页的 log日志，支持重置记录。

   ![image-20250915234553171](C:\Users\74187\AppData\Roaming\Typora\typora-user-images\image-20250915234553171.png)

7. 更新功能（UPDATE）："UPDATE students SET name='H' WHERE id = 3 AND age >= 18;"。

   ![image-20250916155430978](C:\Users\74187\AppData\Roaming\Typora\typora-user-images\image-20250916155430978.png)

8. 谓词下推功能，两个条件都被下推到扫描阶段，形成过滤链，逐层减少数据量。

   ![image-20250915234108451](C:\Users\74187\AppData\Roaming\Typora\typora-user-images\image-20250915234108451.png)

9. 智能纠错提示。

   ![image-20250915234357779](C:\Users\74187\AppData\Roaming\Typora\typora-user-images\image-20250915234357779.png)