import psycopg
import click
import sqlparse


# 数据库连接配置
DB_CONFIG = {
    "dbname": "project2025",
    "user": "postgres",
    "password": "@Sissy803926",
    "host": "localhost",
    "port": "5432"
}

# 创建数据库
def init_database():
    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                try:
                    cur.execute(f"CREATE DATABASE {DB_CONFIG['dbname']};")
                    click.echo(f"数据库 {DB_CONFIG['dbname']} 创建成功！")
                except psycopg.errors.DuplicateDatabase:
                    click.echo(f"数据库 {DB_CONFIG['dbname']} 已存在，跳过创建。")
    except Exception as e:
        click.echo(f"初始化失败：{str(e)}")
        return
init_database()

# 在数据库中导入关系模式
def import_relation():
    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                schema = """
                    CREATE TABLE IF NOT EXISTS users (
                        user_id SERIAL PRIMARY KEY,
                        username VARCHAR(50) NOT NULL,
                        phone_num VARCHAR(50) NOT NULL,
                        e_mail VARCHAR(50)
                    );

                    CREATE TABLE IF NOT EXISTS devices (
                        device_id VARCHAR(100) PRIMARY KEY,
                        device_name VARCHAR(100) NOT NULL,
                        device_type VARCHAR(100) NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS usage_records (
                        record_id SERIAL PRIMARY KEY,
                        user_id INT REFERENCES users(user_id),
                        device_id VARCHAR(100) REFERENCES devices(device_id),
                        start_time TIMESTAMP NOT NULL,
                        end_time TIMESTAMP NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS security_events (
                        event_id SERIAL PRIMARY KEY,
                        user_id INT REFERENCES users(user_id),
                        device_id VARCHAR(100) REFERENCES devices(device_id),
                        event_time TIMESTAMP NOT NULL,
                        event_type VARCHAR(100) NOT NULL,
                        event_describe TEXT
                    );

                    CREATE TABLE IF NOT EXISTS user_feedbacks (
                        feedback_id SERIAL PRIMARY KEY,
                        user_id INT REFERENCES users(user_id),
                        content TEXT NOT NULL
                    );
                    """
                for stmt in sqlparse.split(schema):
                    stmt = stmt.strip()
                    if stmt:
                        try:
                            cur.execute(stmt)
                        except Exception as e:
                            click.echo(f"执行 SQL 失败：{stmt}\n错误：{str(e)}")
            click.echo("关系模式录入完成！")
    except Exception as e:
        click.echo(f"关系模式建立失败：{str(e)}")
import_relation()

# 重置系统
"""
def reset_system():
    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                try:
                    cur.execute(f"DROP DATABASE {DB_CONFIG['dbname']};")
                    click.echo(f"数据库 {DB_CONFIG['dbname']} 已删除。")
                except psycopg.errors.ObjectInUse:
                    click.echo("删除失败：数据库仍有连接，请关闭后重试。")
    except Exception as e:
        click.echo(f"重置失败：{str(e)}")
        return
reset_system()
"""