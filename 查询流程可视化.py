import psycopg
from graphviz import Digraph
import os
import matplotlib.pyplot as plt
from matplotlib.image import imread
import io
from PIL import Image

# 设置Graphviz路径（根据实际安装路径修改）
os.environ["PATH"] += os.pathsep + r'D:\python软件\Graphviz\bin'


class QueryPlanVisualizer:
    """PostgreSQL查询计划可视化工具，强制图例位于最右侧"""

    def __init__(self, db_config):
        """初始化数据库连接配置"""
        self.db_config = db_config
        # 定义不同操作类型的颜色映射
        self.OPERATOR_COLORS = {
            'Seq Scan': '#FFB6C1',  # 浅红色 - 顺序扫描
            'Index Scan': '#90EE90',  # 浅绿色 - 索引扫描
            'Bitmap Heap Scan': '#F0E68C',  # 浅黄色 - 位图堆扫描
            'Hash Join': '#DDA0DD',  # 淡紫色 - Hash连接
            'Nested Loop': '#87CEEB',  # 浅蓝色 - 嵌套循环
            'Aggregate': '#FFA07A',  # 亮珊瑚色 - 聚合操作
            'Sort': '#F5DEB3',  # 小麦色 - 排序操作
            'CTE Scan': '#B0C4DE',  # 亮钢蓝色 - CTE扫描
            'Subquery Scan': '#FFCC99',  # 橙色 - 子查询
            'Materialize': '#C8E6C9',  # 浅绿色 - 物化
            'Function Scan': '#BCAAA4',  # 棕色 - 函数扫描
            'Default': '#E0E0E0'  # 灰色 - 其他操作
        }
        # 配置matplotlib中文显示
        plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
        plt.rcParams["axes.unicode_minus"] = False

    def get_query_plan(self, query):
        """获取查询执行计划"""
        try:
            with psycopg.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    # cur.execute("SET enable_hashjoin = off")
                    cur.execute("SET enable_nestloop = off")
                    cur.execute("SET enable_mergejoin = off")
                    cur.execute("SET enable_bitmapscan = off")
                    # 执行EXPLAIN获取查询计划
                    explain_query = f"EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) {query}"
                    cur.execute(explain_query)
                    plan = cur.fetchone()[0][0]
                    return plan
        except Exception as e:
            print(f"获取查询计划失败: {str(e)}")
            return None

    def _get_node_color(self, node_type):
        """根据节点类型获取颜色"""
        for key in self.OPERATOR_COLORS:
            if key in node_type:
                return self.OPERATOR_COLORS[key]
        return self.OPERATOR_COLORS['Default']

    def _process_node(self, node, parent_id=None, level=0):
        """递归处理查询计划节点，构建图形"""
        node_id = str(id(node))  # 使用对象ID作为唯一标识
        node_type = node['Node Type']
        color = self._get_node_color(node_type)

        # 构建节点标签
        label = [f"<b>{node_type}</b>"]

        # 添加关系名（如果有）
        if 'Relation Name' in node:
            label.append(f"表: {node['Relation Name']}")

        # 添加成本信息
        cost_info = f"成本: {node['Startup Cost']:.2f}..{node['Total Cost']:.2f}"
        if 'Actual Startup Time' in node:
            cost_info += f"\n时间: {node['Actual Total Time']:.2f}ms"
        label.append(cost_info)

        # 添加行数信息
        rows_info = f"计划行数: {node['Plan Rows']}"
        if 'Actual Rows' in node:
            rows_info += f" / 实际: {node['Actual Rows']}"
        label.append(rows_info)

        # 添加过滤条件（如果有）
        if 'Filter' in node:
            filter_text = node['Filter']
            if len(filter_text) > 40:
                filter_text = filter_text[:37] + "..."
            label.append(f"过滤: {filter_text}")

        # 创建HTML格式的标签
        html_label = f"""<<table border="0" cellborder="1" cellspacing="0">
                          <tr><td bgcolor="{color[1:]}">{label[0]}</td></tr>
                          {'<hr/>'.join([f'<tr><td align="left">{line}</td></tr>' for line in label[1:]])}
                        </table>>"""

        # 添加节点到图形
        self.graph.node(node_id, label=html_label, style='filled', fillcolor=color,
                        fontname='SimHei', fontsize='10')

        # 如果有父节点，添加边
        if parent_id:
            self.graph.edge(parent_id, node_id, color='#666666', penwidth='1.0')

        # 递归处理子节点
        if 'Plans' in node:
            for child in node['Plans']:
                self._process_node(child, node_id, level + 1)

    def visualize(self, query, format='png'):
        """可视化查询计划并直接显示，强制图例位于最右侧"""
        plan = self.get_query_plan(query)
        if not plan:
            return None

        # 创建有向图（增加宽度以容纳右侧图例）
        self.graph = Digraph(comment='查询执行计划', format=format)
        self.graph.attr(rankdir='TB', size='20,8', dpi='600')  # 增加宽度
        self.graph.attr('node', shape='box', fontname='SimHei')

        # 处理根节点
        self._process_node(plan['Plan'])

        # 优化图例：右侧垂直排列，强制位于最右侧
        with self.graph.subgraph(name='cluster_legend') as legend:
            legend.attr(
                label='图例',
                fontname='SimHei',
                style='filled',
                fillcolor='#f9f9f9',
                penwidth='0.5',  # 减小边框粗细
                margin='5,5',  # 减小边距
                rankdir='TB',  # 垂直排列
                nodesep='0.2',  # 节点间距
                ranksep='0.2',  # 行间距
                fontsize='12',  # 标题字体
                labeljust='l',  # 标签左对齐
                labelloc='t',  # 标签顶部
                # 强制位于最右侧
                pos='10,0',  # 设置初始位置（右侧）
                width='2',  # 子图宽度
                height='0',  # 自动高度
                bgcolor='transparent'  # 透明背景
            )

            # 创建图例节点
            legend_nodes = []
            for op_type, color in self.OPERATOR_COLORS.items():
                if op_type != 'Default':
                    node_name = f'legend_{op_type}'
                    legend.node(
                        node_name,
                        op_type,
                        style='filled',
                        fillcolor=color,
                        fontname='SimHei',
                        fontsize='12',  # 字体大小
                        height='0.4',  # 节点高度
                        width='1.8',  # 节点宽度
                        fixedsize='true',  # 固定大小
                        margin='0.1,0.1',  # 内边距
                        penwidth='0.5'  # 边框粗细
                    )
                    legend_nodes.append(node_name)

            # 添加 invisible 边强制垂直排列
            for i in range(len(legend_nodes) - 1):
                legend.edge(legend_nodes[i], legend_nodes[i + 1], style='invisible')

        # 渲染图形并获取二进制数据
        try:
            img_data = self.graph.pipe(format=format)
            if not img_data:
                raise Exception("渲染图形时获取数据失败")

            # 使用matplotlib显示图像
            plt.figure(figsize=(22, 10))  # 增加显示宽度
            img = Image.open(io.BytesIO(img_data))
            plt.imshow(img)
            plt.title("PostgreSQL查询执行计划", fontsize=14)
            plt.axis('off')
            plt.tight_layout()
            plt.show()

            return img
        except Exception as e:
            print(f"显示图形失败: {str(e)}")
            return None


# 使用示例
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
    test_query1 = """
    SELECT u.user_id,u.username,COUNT(u.user_id) AS use_time
    FROM users u
    LEFT JOIN usage_records ur USING(user_id)
    WHERE u.username LIKE 'S%'
    GROUP BY u.user_id, u.username
    HAVING u.user_id > 5
    ORDER BY u.user_id
    """
    test_query2 = """
    SELECT u.username,u.user_id,COUNT(ur.device_id)
    FROM users u
    JOIN usage_records ur USING(user_id)
    GROUP BY u.username,u.user_id
    HAVING COUNT(device_id) = (SELECT MAX(count) FROM (SELECT COUNT(device_id) AS count FROM usage_records GROUP BY user_id))
    """
    # 创建可视化器并显示查询计划图
    visualizer = QueryPlanVisualizer(db_config)
    visualizer.visualize(test_query1)
    visualizer.visualize(test_query2)