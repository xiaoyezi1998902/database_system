-- ===========================================
-- 数据库系统测试用例集合
-- 测试范围：词法分析、语法分析、语义分析、执行计划、存储引擎
-- ===========================================

-- ===========================================
-- 1. 基础表创建测试
-- ===========================================

-- 1.1 正常创建表
CREATE TABLE student(id INT, name TEXT, age INT, class_id INT);

-- 1.2 创建班级表
CREATE TABLE class(id INT, name TEXT, teacher TEXT);

-- 1.3 创建成绩表
CREATE TABLE score(student_id INT, subject TEXT, score INT);

-- 1.4 重复创建表（应该报错）
CREATE TABLE student(id INT, name TEXT);

-- ===========================================
-- 2. 数据插入测试
-- ===========================================

-- 2.1 单行插入
INSERT INTO student VALUES (1, 'Alice', 20, 1);

-- 2.2 多行插入
INSERT INTO student VALUES (2, 'Bob', 19, 1), (3, 'Charlie', 21, 2), (4, 'David', 20, 1);

-- 2.3 指定列插入
INSERT INTO student(id, name, age) VALUES (5, 'Eve', 22);

-- 2.4 插入班级数据
INSERT INTO class VALUES (1, 'Math Class', 'Mr. Smith'), (2, 'Science Class', 'Ms. Johnson');

-- 2.5 插入成绩数据
INSERT INTO score VALUES (1, 'Math', 95), (1, 'Science', 88), (2, 'Math', 87), (2, 'Science', 92);

-- ===========================================
-- 3. 基础查询测试
-- ===========================================

-- 3.1 查询所有列
SELECT * FROM student;

-- 3.2 查询指定列
SELECT id, name FROM student;

-- 3.3 条件查询
SELECT * FROM student WHERE age > 20;

-- 3.4 多条件查询
SELECT * FROM student WHERE age >= 20 AND class_id = 1;

-- 3.5 排序查询
SELECT * FROM student ORDER BY age DESC;

-- ===========================================
-- 4. JOIN查询测试
-- ===========================================

-- 4.1 内连接
SELECT s.name, c.name as class_name 
FROM student s 
JOIN class c ON s.class_id = c.id;

-- 4.2 左连接
SELECT s.name, c.name as class_name 
FROM student s 
LEFT JOIN class c ON s.class_id = c.id;

-- ===========================================
-- 5. 更新操作测试
-- ===========================================

-- 5.1 更新单列
UPDATE student SET age = 21 WHERE id = 1;

-- 5.2 更新多列
UPDATE student SET age = 22, class_id = 2 WHERE name = 'Alice';

-- ===========================================
-- 6. 删除操作测试
-- ===========================================

-- 6.1 条件删除
DELETE FROM student WHERE id = 5;

-- 6.2 删除特定记录
DELETE FROM student WHERE age < 20;

-- 6.3 查询删除后的结果
SELECT * FROM student;

-- ===========================================
-- 7. 错误语句测试（应该报错）
-- ===========================================

-- 7.1 缺少分号
CREATE TABLE test1(id INT, name TEXT)

-- 7.2 列名错误
SELECT non_existent_column FROM student;

-- 7.3 表名错误
SELECT * FROM non_existent_table;

-- 7.4 类型不匹配
INSERT INTO student VALUES ('invalid_id', 'Test', 20, 1);

-- 7.5 字符串未闭合
INSERT INTO student VALUES (6, 'Unclosed string', 20, 1);

-- 7.6 语法错误
SELCT * FROM student;

-- 7.7 缺少关键字
CREATE student(id INT, name TEXT);

-- 7.8 重复列名
CREATE TABLE test2(id INT, id TEXT);

-- 7.9 不支持的类型
CREATE TABLE test3(id CUSTOM_TYPE, name TEXT);

-- 7.10 值个数不匹配
INSERT INTO student VALUES (7, 'Test');

-- 7.11 不存在的列
UPDATE student SET non_existent = 'value' WHERE id = 1;

-- 7.12 不存在的表
UPDATE non_existent_table SET name = 'test' WHERE id = 1;

-- ===========================================
-- 8. 性能测试（大量数据）
-- ===========================================

-- 8.1 批量插入大量数据
INSERT INTO student VALUES 
(100, 'Student100', 20, 1), (101, 'Student101', 21, 1), (102, 'Student102', 20, 2),
(103, 'Student103', 22, 1), (104, 'Student104', 19, 2), (105, 'Student105', 21, 1),
(106, 'Student106', 20, 2), (107, 'Student107', 22, 1), (108, 'Student108', 19, 2),
(109, 'Student109', 21, 1), (110, 'Student110', 20, 2), (111, 'Student111', 22, 1),
(112, 'Student112', 19, 2), (113, 'Student113', 21, 1), (114, 'Student114', 20, 2),
(115, 'Student115', 22, 1), (116, 'Student116', 19, 2), (117, 'Student117', 21, 1),
(118, 'Student118', 20, 2), (119, 'Student119', 22, 1), (120, 'Student120', 19, 2);

-- 8.2 大量数据排序
SELECT * FROM student ORDER BY name;

-- ===========================================
-- 9. 聚合函数测试
-- ===========================================

-- 9.1 基础聚合函数测试
SELECT COUNT(*) FROM student;

-- 9.2 按列计数
SELECT COUNT(class_id) FROM student;

-- 9.3 求和
SELECT SUM(age) FROM student;

-- 9.4 平均值
SELECT AVG(age) FROM student;

-- 9.5 最大值
SELECT MAX(age) FROM student;

-- 9.6 最小值
SELECT MIN(age) FROM student;

-- 9.7 多个聚合函数
SELECT COUNT(*), AVG(age), MAX(age), MIN(age) FROM student;

-- 9.8 带别名的聚合函数
SELECT COUNT(*) as total_students, AVG(age) as avg_age FROM student;

-- ===========================================
-- 10. GROUP BY 分组测试
-- ===========================================

-- 10.1 按班级分组
SELECT class_id, COUNT(*) FROM student GROUP BY class_id;

-- 10.2 按班级分组，显示平均年龄
SELECT class_id, COUNT(*) as student_count, AVG(age) as avg_age FROM student GROUP BY class_id;

-- 10.3 按年龄分组
SELECT age, COUNT(*) as count_num FROM student GROUP BY age;

-- 10.4 多列分组
SELECT class_id, age, COUNT(*) FROM student GROUP BY class_id, age;

-- ===========================================
-- 测试完成
-- ===========================================