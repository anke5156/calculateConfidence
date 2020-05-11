## 一键执行
`python scheJob.py`


## 手动分步执行
 1. 构建mapping  
`python calculateConfidence.py DrawMapping build_from_db`
2. 构建sql脚本  
`python calculateConfidence.py DrawSql start`
3. 执行sql脚本  
`python scheJob.py`

