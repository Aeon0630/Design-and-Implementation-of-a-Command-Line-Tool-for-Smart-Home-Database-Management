import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError
import psycopg
from psycopg import sql
from typing import Dict, List, Any, Optional, Tuple, Union
import re

class SQLQueryValidator:
    """
    SQL 查询验证器，用于检查 SQL 查询的正确性并提供友好的错误信息和修改建议
    """
    def __init__(self, dialect: str = "postgres", db_config: Optional[Dict[str, str]] = None):
        self.dialect = dialect
        self.db_config = db_config
        self.metadata = {}
        self.parsed = None
        self.query = ""
        # 连接数据库并加载元数据（如果提供了配置）
        if db_config:
            self._load_metadata()

    def _load_metadata(self) -> None:
        """加载数据库元数据（表和列信息）"""
        try:
            with psycopg.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    # 获取所有表
                    cur.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                    """)
                    tables = [row[0] for row in cur.fetchall()]
                    # 获取每个表的列信息
                    for table in tables:
                        cur.execute(f"""
                            SELECT column_name, data_type 
                            FROM information_schema.columns 
                            WHERE table_name = %s
                        """, (table,))
                        columns = {row[0]: row[1] for row in cur.fetchall()}
                        self.metadata[table] = columns
                    print(self.metadata)
                    column_types = {
                        f"{table}.{column}": data_type
                        for table, columns in self.metadata.items()
                        for column, data_type in columns.items()
                    }
        except Exception as e:
            print(f"警告: 无法加载数据库元数据: {e}")

    def validate(self, query: str) -> Dict[str, Any]:
        """验证 SQL 查询的正确性"""
        self.query = query
        result = {"is_valid": True, "errors": [], "suggestions": []}
        # 1. 基本语法检查
        try:
            self.parsed = sqlglot.parse_one(query, dialect=self.dialect)
        except ParseError as e:
            result["is_valid"] = False
            result["errors"].append({
                "type": "语法错误",
                "message": str(e),
                "suggestion": "检查 SQL 关键字拼写和语句结构，确保标点符号正确"
            })
        # 2. 检查 SELECT 语句结构
        if isinstance(self.parsed, exp.Select):
            result = self._validate_select_statement(result)
        # 3. 检查 JOIN 条件
        result = self._validate_joins(result)
        # 4. 检查 WHERE 语句中的问题
        result = self._validate_where(result)
        # 5. 检查 HAVING 语句中的问题
        result = self._validate_having(result)
        # 6. 检查表和列是否存在（如果有元数据）
        if self.metadata:
            result = self._validate_tables_and_columns(result)

        return result

    def _validate_select_statement(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """验证 SELECT 语句结构"""
        # 检查 FROM 子句是否缺失
        if not self.parsed.args.get("from"):
            result["is_valid"] = False
            result["errors"].append({
                "type": "结构错误",
                "message": "SELECT 语句缺少 FROM 子句",
                "suggestion": "添加 FROM 子句指定数据源"
            })
        # 检查 GROUP BY 子句是否有误
        aggregates = list(self.parsed.find_all(exp.AggFunc))
        selected_columns = [
            e for e in self.parsed.expressions
            if isinstance(e, exp.Column) and not isinstance(e.parent, exp.AggFunc)
        ]
        # 获取GROUP BY子句中的列
        group_by_clause = self.parsed.args.get("group")
        group_by_columns = []
        if group_by_clause:
            group_by_columns = [
                expr for expr in group_by_clause.expressions
                if isinstance(expr, exp.Column)
            ]
        # 情况1：有聚合函数和非聚合列，但没有GROUP BY
        if aggregates and selected_columns and not self.parsed.args.get("group"):
            result["is_valid"] = False
            result["errors"].append({
                "type": "结构错误",
                "message": "SELECT 列表中同时包含聚合函数和普通列，但缺少 GROUP BY 子句",
                "suggestion": "添加 GROUP BY 子句，或移除普通列只保留聚合函数"
            })
        # 情况2：有GROUP BY，但SELECT中的非聚合列未全部包含在GROUP BY中
        if group_by_clause and selected_columns:
            # 提取列名（含表名）用于比较
            select_col_names = {f"{col.table}.{col.name}" if col.table else col.name for col in selected_columns}
            group_by_col_names = {f"{col.table}.{col.name}" if col.table else col.name for col in group_by_columns}

            # 检查是否所有SELECT中的非聚合列都在GROUP BY中
            missing_columns = select_col_names - group_by_col_names
            if missing_columns:
                result["is_valid"] = False
                result["errors"].append({
                    "type": "GROUP BY不完整",
                    "message": f"SELECT列表中的普通列 {', '.join(missing_columns)} 未包含在GROUP BY子句中",
                    "suggestion": f"在GROUP BY子句中添加列：{', '.join(missing_columns)}"
                })
        return result

    def _validate_joins(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """验证 JOIN 条件，支持自联结和 USING 子句校验"""
        joins = self.parsed.args.get("joins", [])
        from_table = self.parsed.find(exp.From)
        base_tables = []  # 记录基础表
        table_aliases = {}  # 记录表的别名
        # 提取 FROM 子句中的基础表、别名
        if from_table:
            for table_ref in from_table.find_all(exp.Table):
                table_name = table_ref.name
                alias = table_ref.alias or table_name
                base_tables.append({"name": table_name, "alias": alias})
                table_aliases[alias] = table_name  # 存储别名映射
        # 从JOIN子句提取表别名
        for join in joins:
            join_table = join.this if hasattr(join, 'this') else join.table
            alias = join.alias_or_name if hasattr(join, 'alias_or_name') else join.alias or join_table.name
            table_name = join_table.name
            table_aliases[alias] = table_name  # 存储JOIN表的别名映射
        for join in joins:
            join_kind = join.kind.upper()
            on_clause = join.args.get("on")
            using_columns = join.args.get("using")
            join_table = join.this if hasattr(join, 'this') else join.table
            # 获取参与连接的表（包括主表和连接表）
            join_tables = []
            # 主表（FROM 子句中的表或前一个 JOIN 的表）
            if not join_tables and base_tables:
                join_tables.extend(base_tables)
            # 连接表
            join_tables.append({"name": join_table.name, "alias": join.alias_or_name if hasattr(join, 'alias_or_name') else join.alias or join_table.name})
            # 校验缺少连接条件（ON/USING 都没有）
            if not on_clause and not using_columns:
                result["is_valid"] = False
                result["errors"].append({
                    "type": "JOIN 错误",
                    "message": f"{join_kind} JOIN 缺少连接条件（需 ON 或 USING 子句）",
                    "suggestion": "添加 ON 子句指定连接条件，或用 USING 子句简化同名字段连接"
                })
                continue  # 缺少条件，后续针对条件里对象的校验暂不做，直接处理下一个 join
            # 处理 USING 子句
            if using_columns:
                for col in using_columns:
                    col_name = col.name
                    # 检查 USING 列在参与连接的表中是否存在
                    valid_tables = []
                    for table_info in join_tables:
                        table_name = table_info["name"]
                        if table_name in self.metadata and col_name in self.metadata[table_name]:
                            valid_tables.append(table_info["alias"])
                    if not valid_tables:
                        result["is_valid"] = False
                        result["errors"].append({
                            "type": "列不存在",
                            "message": f"USING 子句中列 '{col_name}' 在参与连接的表 {[t['alias'] for t in join_tables]} 中均不存在",
                            "suggestion": f"可用列: {', '.join([f'{t}: {', '.join(self.metadata[t])}' for t in self.metadata if t in [t['name'] for t in join_tables]])}"
                        })
                    elif len(valid_tables) < 2:
                        result["is_valid"] = False
                        result["errors"].append({
                            "type": "JOIN 错误",
                            "message": f"USING 子句中列 '{col_name}' 仅在表 {valid_tables[0]} 中存在，无法建立连接",
                            "suggestion": "确保该列在连接的两个表中都存在，或改用 ON 子句指定不同列"
                        })
                continue
            # 处理 ON 子句
            if on_clause:
                for column in on_clause.find_all(exp.Column):
                    col_table = column.table
                    col_name = column.name
                    # 解析表名/别名对应的真实表（使用新提取的 table_aliases）
                    real_table = col_table
                    if col_table in table_aliases:
                        real_table = table_aliases[col_table]
                    # 检查表是否存在（参与连接的表中包含该表）
                    table_exists = any(t["name"] == real_table for t in join_tables)
                    if not table_exists:
                        result["is_valid"] = False
                        result["errors"].append({
                            "type": "对象不存在",
                            "message": f"ON 子句中引用的表 '{col_table}' 未在参与连接的表中定义",
                            "suggestion": f"参与连接的表: {', '.join([t['alias'] for t in join_tables])}，检查表名/别名拼写"
                        })
                        continue
                    # 检查表中列是否存在
                    if real_table in self.metadata and col_name in self.metadata[real_table]:
                        continue
                    result["is_valid"] = False
                    result["errors"].append({
                        "type": "列不存在",
                        "message": f"表 '{real_table}' 中没有列 '{col_name}'",
                        "suggestion": f"可用列: {', '.join(self.metadata[real_table].keys()) if real_table in self.metadata else '无可用列信息'}"
                    })
        return result


    def _validate_where(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """验证 WHERE 子句中的类型不匹配和空值比较错误"""
        where_clause = self.parsed.args.get("where")
        if not where_clause:
            return result
        # 直接从元数据构建完整列名到类型的映射
        column_types = {
            f"{column}": data_type
            for table, columns in self.metadata.items()
            for column, data_type in columns.items()
        }
        # 遍历 WHERE 子句中的所有比较操作
        for condition in where_clause.find_all(exp.Binary):
            left = condition.left
            right = condition.right
            print(left)
            print(right)
            # 1. 检查类型不匹配（列与字面量比较）
            if isinstance(left, exp.Column) and isinstance(right, exp.Literal):
                # 构建完整列名
                column_name = left.name
                # 从预构建的字典中获取列类型
                column_type = column_types.get(column_name)
                if not column_type:
                    continue  # 列不存在，跳过检查
                # 数值列与非数字字符串比较
                if (
                        column_type.startswith("integer")
                        and right.is_string
                        and not right.this.replace('.', '', 1).isdigit()
                ):
                    result["is_valid"] = False
                    result["errors"].append({
                        "type": "类型不匹配",
                        "message": f"数值列 '{column_name}' 不能与非数字字符串 '{right.this}' 比较",
                        "suggestion": "移除字符串引号或使用 CAST 函数"
                    })

                # 字符串列与非字符串值比较
                elif (
                        column_type.startswith("character")
                        and not right.is_string
                ):
                    result["is_valid"] = False
                    result["errors"].append({
                        "type": "类型不匹配",
                        "message": f"字符串列 '{column_name}' 不能与非字符串值比较",
                        "suggestion": "将值用引号包裹"
                    })

            # 2. 检查空值比较错误（= NULL 或 <> NULL）
            is_null_value = False
            if isinstance(right, exp.Null):
                is_null_value = True
            elif isinstance(right, exp.Identifier) and right.name.upper() == "NULL":
                is_null_value = True

            if is_null_value and condition.type not in ["IS", "IS NOT"]:
                result["is_valid"] = False
                result["errors"].append({
                    "type": "空值比较错误",
                    "message": "空值比较应使用 'IS NULL' 或 'IS NOT NULL'",
                    "suggestion": f"将 '{condition.type}' 替换为 'IS NULL' 或 'IS NOT NULL'"
                })

        return result

    def _validate_having(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """验证 HAVING 子句中的错误"""
        having_clause = self.parsed.args.get("having")
        if not having_clause:
            return result
        # 1. 检查是否误用 HAVING 替代 WHERE（没有 GROUP BY 的情况）
        if not self.parsed.args.get("group"):
            aggregates = list(having_clause.find_all(exp.AggFunc))
            if not aggregates:
                result["is_valid"] = False
                result["errors"].append({
                    "type": "误用HAVING",
                    "message": "查询没有 GROUP BY 子句，但使用了 HAVING 而非 WHERE",
                    "suggestion": "将 HAVING 子句改为 WHERE 子句"
                })
        # 2. 获取 SELECT 列表中的列（包括别名）
        select_columns = set()
        for expr in self.parsed.selects:
            # 处理带别名的表达式
            if isinstance(expr, exp.Alias):
                alias = expr.alias
                select_columns.add(alias)
                inner_expr = expr.this
                if isinstance(inner_expr, exp.Column):
                    select_columns.add(inner_expr.name)
                    if inner_expr.table:
                        select_columns.add(f"{inner_expr.table}.{inner_expr.name}")
            # 处理普通列
            elif isinstance(expr, exp.Column):
                select_columns.add(expr.name)
                if expr.table:
                    select_columns.add(f"{expr.table}.{expr.name}")
            # 处理聚合函数（跳过，因为聚合函数不需要出现在 SELECT 中）
            elif isinstance(expr, exp.AggFunc):
                pass
            # 处理其他表达式（记录别名或原始表达式文本）
            else:
                if expr.alias:
                    select_columns.add(expr.alias)
                else:
                    select_columns.add(str(expr))
            # 3.检查 HAVING 子句中的所有非聚合列
            for column in having_clause.find_all(exp.Column):
                # 跳过作为聚合函数参数的列（如 SUM(col) 中的 col）
                if isinstance(column.parent, exp.AggFunc):
                    continue
                # 构建列的完整名称（带表名）和简单名称
                full_name = f"{column.table}.{column.name}" if column.table else column.name
                simple_name = column.name
                # 检查列是否存在于 SELECT 列表中
                if full_name not in select_columns and simple_name not in select_columns:
                    result["is_valid"] = False
                    result["errors"].append({
                        "type": "非法HAVING列",
                        "message": f"HAVING 子句中引用了不在 SELECT 列表中的非聚合列 '{full_name}'",
                        "suggestion": "将列添加到 SELECT 列表中"
                    })
        return result

    def _extract_tables_and_columns(self) -> Dict[str, Any]:
        """从SQL查询中提取所有表、列和表别名"""
        tables = []  # 存储表名
        columns = []  # 存储列名（包括聚合函数中的列）
        table_aliases = {}  # 存储表别名到真实表名的映射
        # 提取FROM子句中的表
        from_clause = self.parsed.find(exp.From)
        if from_clause:
            for table_ref in from_clause.find_all(exp.Table):
                table_name = table_ref.name
                alias = table_ref.alias or table_name
                tables.append(table_name)
                table_aliases[alias] = table_name
        # 提取JOIN子句中的表
        for join in self.parsed.find_all(exp.Join):
            # 处理表名
            join_table = join.this if hasattr(join, 'this') else join.table
            table_name = join_table.name
            alias = join.alias_or_name if hasattr(join, 'alias_or_name') else join.alias
            tables.append(table_name)
            table_aliases[alias] = table_name
        # 提取所有列引用（包括聚合函数中的列）
        for column in self.parsed.find_all(exp.Column):
            col_name = column.name
            columns.append(col_name)
        # 去重处理
        tables = list(set(tables))
        columns = list(set(columns))
        return {
            "tables": tables,
            "columns": columns,
            "table_aliases": table_aliases
        }

    def _validate_tables_and_columns(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """验证表和列是否存在"""
        # 提取所有表、列和表别名
        extraction_result = self._extract_tables_and_columns()
        tables = extraction_result["tables"]
        columns = extraction_result["columns"]
        table_aliases = extraction_result["table_aliases"]
        all_columns = set()
        for table, cols in self.metadata.items():
            all_columns.update(cols.keys())
        all_columns = list(all_columns)
        # 验证表存在性
        for table in tables:
            if table not in self.metadata.keys():
                result["is_valid"] = False
                result["errors"].append({
                    "type": "表不存在",
                    "message": f"找不到名为 '{table}' 的表",
                    "suggestion": f"可用表: {', '.join(self.metadata.keys())}"
                })
        # 验证列存在性
        for column in columns:
            if column not in all_columns:
                result["is_valid"] = False
                result["errors"].append({
                    "type": "列不存在",
                    "message": f"找不到名为 '{column}' 的列",
                    "suggestion": f"可用列: {', '.join(all_columns)}"
                })
        return result

    def get_formatted_errors(self, result: Dict[str, Any]) -> str:
        """获取格式化的错误信息字符串"""
        if result["is_valid"]:
            return "✅ 查询验证通过，未发现错误"
        error_str = "❌ 查询存在错误:\n"
        for i, error in enumerate(result["errors"], 1):
            error_str += f"\n错误 {i}: {error['type']}\n"
            error_str += f"  信息: {error['message']}\n"
            error_str += f"  建议: {error['suggestion']}\n"
        return error_str


if __name__ == "__main__":
    # 查询示例
    query = ""
    # 数据库连接配置
    validator = SQLQueryValidator(
        dialect="postgres",
        db_config={
            "dbname": "project2025",
            "user": "postgres",
            "password": "@Sissy803926",
            "host": "localhost",
            "port": "5432"
        })
    result = validator.validate(query)
    print(result)
    print(validator.get_formatted_errors(result))