import os
import psycopg
import pandas as pd
import matplotlib.pyplot as plt
import requests
import json
import re

# 配置 matplotlib 中文显示
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
plt.rcParams["axes.unicode_minus"] = False

class DeepSeekNL2SQLExecutor:
    def __init__(self, db_config, api_key):
        self.db_config = db_config
        self.api_key = api_key
        self.api_base = "https://api.deepseek.com/v1/chat/completions"  # DeepSeek API 端点

        # 定义 SQL 转换提示模板，明确要求不返回Markdown格式
        self.prompt_template = """
        作为专业的 PostgreSQL 数据库专家，请将以下自然语言查询转换为 SQL 语句。
        数据库表结构：
        - users (user_id, username, phone_num, e_mail)
        - devices (device_id, device_name, device_type)
        - usage_records (record_id, user_id, device_id, start_time, end_time)

        要求：
        1. 仅返回可执行的纯SQL语句，不包含任何Markdown格式（如```sql或```）
        2. 使用标准 PostgreSQL 语法
        3. 确保表名和字段名正确

        自然语言查询：{question}
        """

    def natural_language_to_sql(self, natural_language):
        """直接调用 DeepSeek API，构造正确的 JSON 格式"""
        # 构建完整提示
        full_prompt = self.prompt_template.format(question=natural_language)

        # 构造符合 DeepSeek API 要求的请求体
        payload = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "user", "content": full_prompt}
            ],
            "temperature": 0.3,
            "max_tokens": 1024
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }

        try:
            response = requests.post(
                self.api_base,
                headers=headers,
                data=json.dumps(payload),
                timeout=30
            )
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"].strip()
        except requests.exceptions.RequestException as e:
            print(f"API 调用失败: {str(e)}")
            print(f"响应状态码: {response.status_code if hasattr(response, 'status_code') else 'N/A'}")
            print(f"响应内容: {response.text if hasattr(response, 'text') else 'N/A'}")
            return None
        except KeyError as e:
            print(f"响应格式错误: {str(e)}，响应内容: {response.json()}")
            return None

    def _clean_sql_format(self, sql):
        """移除 SQL 中的 Markdown 格式符号及其他多余内容"""
        # 移除 ```sql、``` 及其他Markdown格式
        sql = re.sub(r'```sql?\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'```', '', sql)

        # 移除行首行尾的空白字符
        sql = sql.strip()

        # 合并连续的空白字符为单个空格
        sql = re.sub(r'\s+', ' ', sql)

        # 移除可能存在的注释
        sql = re.sub(r'--.*?(\n|$)', '', sql, flags=re.DOTALL)  # 移除单行注释
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)  # 移除多行注释

        return sql

    def execute_sql(self, sql):
        """执行 SQL 并返回结果"""
        # 清理SQL格式
        cleaned_sql = self._clean_sql_format(sql)
        print(f"执行的SQL: {cleaned_sql}")  # 打印清理后的SQL

        try:
            with psycopg.connect(**self.db_config) as conn:
                return pd.read_sql(cleaned_sql, conn)
        except Exception as e:
            print(f"执行 SQL 失败: {str(e)}")
            return None

    def execute_query(self, natural_language):
        """执行自然语言查询并展示结果表格"""
        # 1. 转换为 SQL
        sql = self.natural_language_to_sql(natural_language)
        if not sql:
            return None, None
        print(f"\n生成的 SQL: {sql}")

        # 2. 执行查询
        result_df = self.execute_sql(sql)
        if result_df is None or result_df.empty:
            print("查询结果为空")
            return sql, None

        # 3. 展示结果表格
        self._display_table(result_df, natural_language)
        return sql, result_df

    def _display_table(self, df, query_title):
        """美化展示查询结果表格"""
        fig, ax = plt.subplots(figsize=(12, min(8, len(df) * 0.3 + 2)))
        ax.axis('off')

        # 创建带样式的表格
        table = ax.table(
            cellText=df.values,
            colLabels=df.columns,
            loc='center',
            cellLoc='center',
            colWidths=[0.2] * len(df.columns)
        )

        # 设置表格样式
        table.set_fontsize(12)
        table.scale(1, 1.5)  # 调整行高

        # 美化表头和行
        for (row, col), cell in table.get_celld().items():
            if row == 0:  # 表头
                cell.set_facecolor('#4F81BD')
                cell.set_text_props(color='white', weight='bold')
            elif row % 2 == 1:  # 奇数行
                cell.set_facecolor('#F2F2F2')
            else:  # 偶数行
                cell.set_facecolor('#E5E5E5')

        plt.title(f"查询结果: {query_title}", fontsize=14, pad=20)
        plt.tight_layout()
        plt.show()


# 使用示例
if __name__ == "__main__":
    # 数据库配置（替换为实际信息）
    db_config = {
        "dbname": "project2025",
        "user": "postgres",
        "password": "@Sissy803926",
        "host": "localhost",
        "port": "5432"
    }
    # 替换为您的 DeepSeek API 密钥（请使用有效的API密钥）
    api_key = "sk-aa175154dc794647b399c6b9d70373b9"
    # 创建执行器
    executor = DeepSeekNL2SQLExecutor(db_config, api_key)
    # 自然语言查询示例
    queries = [
        """根据相关数据库的结构，查询每个用户的ID、名字和所有使用过的设备
           数据库表结构如下：
            - users (user_id, username, phone_num, e_mail)
            - devices (device_id, device_name, device_type)
            - usage_records (record_id, user_id, device_id, start_time, end_time)
        """
    ]
    # 执行查询
    for query in queries:
        executor.execute_query(query)