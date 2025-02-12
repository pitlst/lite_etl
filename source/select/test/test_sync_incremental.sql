SELECT 
    f.id AS '员工编号',
    f.name AS '员工姓名',
    f.age,
    f.department AS '部门',
    f.modification_time AS '最后修改时间'
FROM test_schema.employee_performance f