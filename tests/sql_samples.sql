-- 测试用 SQL 脚本（正确 + 错误语句）
CREATE TABLE student(id INT, name TEXT, age INT);
INSERT INTO student(id,name,age) VALUES (1,'Alice',20);
INSERT INTO student(id,name,age) VALUES (2,'Bob',19);
SELECT id,name FROM student WHERE age > 18;
DELETE FROM student WHERE id = 1;

-- 错误语句
DELETE FRMO student WHERE id = 2;