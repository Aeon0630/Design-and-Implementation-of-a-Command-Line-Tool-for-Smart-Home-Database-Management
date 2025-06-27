import psycopg
from psycopg.rows import dict_row
from tabulate import tabulate
import matplotlib.pyplot as plt
from matplotlib.table import Table
import sys

# 配置 matplotlib 中文显示
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
plt.rcParams["axes.unicode_minus"] = False

def execute_postgres_query(query, db_config, params=None, fetch_all=True, format_table=True):
    try:
        with psycopg.connect(**db_config) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, params or ())
                if query.strip().upper().startswith("SELECT"):
                    result = cur.fetchall() if fetch_all else cur.fetchone()
                    if format_table:
                        # 处理单条记录
                        if not fetch_all and result:
                            return tabulate([result], headers="keys", tablefmt="psql")
                        # 处理多条记录
                        elif result:
                            return tabulate(result, headers="keys", tablefmt="psql")
                        else:
                            return "查询结果为空"
                    else:
                        return result
                else:
                    return f"受影响行数: {cur.rowcount}"
    except psycopg.Error as e:
        print(f"[数据库错误] {str(e)}", file=sys.stderr)
        return None  # 数据库错误时返回None
    except Exception as e:
        print(f"[未知错误] {str(e)}", file=sys.stderr)
        return None  # 未知错误时返回None

def parse_tabulate_psql(text_table):
    """解析 tabulate 生成的 psql 格式表格，提取表头和数据"""
    if text_table is None:
        return [], []  # 防止None类型调用splitlines()
    lines = [line for line in text_table.splitlines() if line.strip()]
    # 提取表头
    header = [col.strip() for col in lines[1].split("|")[1:-1]] if len(lines) > 1 else []
    # 提取数据行
    data = []
    if len(lines) > 3:
        for line in lines[3:-1]:
            row_data = [col.strip() for col in line.split("|")[1:-1]]
            data.append(row_data)
    return header, data

def create_table_image(header, data, font_size=20, dpi=300):
    """生成大字体表格图片（优化显示效果）"""
    if not header or not data:
        print("数据或表头为空，无法生成图片")
        return
    # 动态计算图片高度（每行数据增加0.5英寸高度）
    rows = len(data) + 1  # 1行表头
    fig, ax = plt.subplots(figsize=(14, max(6, rows * 0.5)), dpi=dpi)
    ax.set_axis_off()
    # 创建表格并填充数据
    table = Table(ax, bbox=[0, 0, 1, 1])
    # 添加表头（加大字体并加粗）
    for col_idx, col_name in enumerate(header):
        cell = table.add_cell(0, col_idx, 1, 1, text=col_name,
                              loc='center', facecolor='lightgray', edgecolor='black')
        cell.get_text().set_fontsize(font_size)
        cell.get_text().set_fontweight('bold')
    # 添加数据行
    for row_idx, row in enumerate(data):
        for col_idx, cell_value in enumerate(row):
            cell = table.add_cell(row_idx + 1, col_idx, 1, 1, text=cell_value,
                                  loc='left', facecolor='white', edgecolor='black')
            cell.get_text().set_fontsize(font_size)

    ax.add_table(table)
    plt.tight_layout()  # 优化布局
    plt.show()

# 示例：以表格形式查询和展示数据
if __name__ == "__main__":
    # 数据库连接配置
    db_config = {
        "dbname": "project2025",
        "user": "postgres",
        "password": "@Sissy803926",
        "host": "localhost",
        "port": "5432"
    }
    # 查询示例
    simple_query = """
    SELECT u.user_id,u.username,ur.device_id,d.device_name,ur.start_time,ur.end_time
    FROM users u
    LEFT JOIN usage_records ur USING(user_id)
    LEFT JOIN devices d USING(device_id)
    LIMIT 10
    """
    # 执行查询（可能返回None）
    table_result = execute_postgres_query(simple_query, db_config)
    # 新增：先判断table_result是否为None
    if table_result is None:
        print("查询执行失败，无法生成图片")
    else:
        print("文本表格：")
        print(table_result)

        # 生成图片表格
        if table_result != "查询结果为空":
            header, data = parse_tabulate_psql(table_result)
            create_table_image(header, data, font_size=20)
        else:
            print("查询结果为空，无需生成图片")