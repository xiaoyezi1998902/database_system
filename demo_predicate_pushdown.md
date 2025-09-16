# 谓词下推功能演示

## 什么是谓词下推？

谓词下推（Predicate Pushdown）是一种查询优化技术，它将WHERE条件尽可能早地应用到数据扫描阶段，减少需要处理的数据量。

## 在我们的数据库系统中的实现

### 1. 基本WHERE条件下推

**SQL查询：**
```sql
SELECT * FROM student WHERE age > 20;
```

**执行计划：**
```
Filter
  predicate: age > 20
  SeqScan
    table: student
```

**优化效果：**
- 在扫描student表时，只返回age > 20的行
- 减少了后续操作需要处理的数据量

### 2. 多条件AND下推

**SQL查询：**
```sql
SELECT * FROM student WHERE age > 19 AND class_id = 1;
```

**执行计划：**
```
Filter
  predicate: class_id = 1
  Filter
    predicate: age > 19
    SeqScan
      table: student
```

**优化效果：**
- 两个条件都被下推到扫描阶段
- 形成过滤链，逐层减少数据量

### 3. JOIN查询中的谓词下推

**SQL查询：**
```sql
SELECT s.name, c.name 
FROM student s 
JOIN class c ON s.class_id = c.id 
WHERE s.age > 20;
```

**执行计划：**
```
Project
  columns: [s.name, c.name]
  Join
    join_type: INNER
    on_condition: s.class_id = c.id
    Filter
      predicate: s.age > 20
      SeqScan
        table: student
    SeqScan
      table: class
```

**优化效果：**
- WHERE条件`s.age > 20`被下推到student表的扫描阶段
- 减少了JOIN操作需要处理的数据量

### 4. 复杂条件处理

**SQL查询：**
```sql
SELECT * FROM student WHERE age >= 20 AND (class_id = 1 OR class_id = 2);
```

**执行计划：**
```
Filter
  predicate: class_id = 2
  Filter
    predicate: class_id = 1
  Filter
    predicate: age >= 20
    SeqScan
      table: student
```

**优化效果：**
- 复杂条件被分解为多个简单条件
- 每个条件都被下推到扫描阶段

## 实现细节

### 1. 条件扁平化 (`_flatten_condition`)

```python
def _flatten_condition(self, condition: Condition) -> List[Comparison]:
    """将复杂条件扁平化为简单比较条件的列表"""
    if isinstance(condition, Comparison):
        return [condition]
    elif isinstance(condition, LogicalExpression):
        left_preds = self._flatten_condition(condition.left)
        right_preds = self._flatten_condition(condition.right)
        if condition.op == 'AND':
            return left_preds + right_preds
        else:  # OR
            return left_preds + right_preds
    return []
```

### 2. 过滤条件添加 (`_add_filters`)

```python
def _add_filters(self, node: PlanNode, predicates: List[Comparison]) -> PlanNode:
    """为节点添加过滤条件"""
    for pred in predicates:
        node = PlanNode("Filter", {"predicate": {
            "left": pred.left,
            "op": pred.op,
            "right": pred.right,
        }}, [node])
    return node
```

### 3. JOIN中的谓词下推

```python
# 将WHERE条件中涉及JOIN表的谓词下推
for pred in where_predicates[:]:
    if self._predicate_involves_table(pred, join.table_name):
        join_predicates.append(pred)
        where_predicates.remove(pred)

# 为JOIN表添加过滤条件
if join_predicates:
    join_scan = self._add_filters(join_scan, join_predicates)
```

## 性能优势

1. **减少数据传输**：在数据源处就过滤掉不需要的数据
2. **减少内存使用**：处理更少的数据行
3. **提高查询效率**：后续操作（如JOIN、GROUP BY）处理的数据量更小
4. **并行处理**：多个过滤条件可以并行执行

## 测试方法

1. 启动服务器：`python server.py`
2. 访问：`http://127.0.0.1:8000`
3. 在SQL编辑器中输入上述查询
4. 点击"Plan"标签查看执行计划
5. 观察Filter节点如何被下推到SeqScan节点附近
