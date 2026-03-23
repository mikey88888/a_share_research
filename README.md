# A-Share Research

WSL + PostgreSQL + FastAPI 的 A 股研究工作区。当前版本已经把看板重构成统一市场入口，支持指数页、个股列表检索、个股独立 K 线研究页。

## 开发

安装依赖：

```bash
uv sync
```

运行基础脚本：

```bash
uv run python scripts/smoke_test.py
```

运行测试：

```bash
export A_SHARE_PG_DSN="postgresql://thinkpad@127.0.0.1:5432/a_share_research"
PYTHONPATH=src ./.venv/bin/python -m unittest discover -s tests
```

运行覆盖率并校验 85% 阈值：

```bash
export A_SHARE_PG_DSN="postgresql://thinkpad@127.0.0.1:5432/a_share_research"
PYTHONPATH=src ./.venv/bin/python -m coverage run -m unittest discover -s tests
./.venv/bin/python -m coverage report -m
```

## 运行

启动本地 PostgreSQL 16：

```bash
./scripts/start_local_postgres.sh
export A_SHARE_PG_DSN="postgresql://thinkpad@127.0.0.1:5432/a_share_research"
```

前台启动看板：

```bash
uv run python -m a_share_research.webapp --host 0.0.0.0 --port 8000
```

后台启动看板：

```bash
./scripts/start_dashboard.sh
./scripts/stop_dashboard.sh
```

停止本地 PostgreSQL：

```bash
./scripts/stop_local_postgres.sh
```

## 数据同步

初始化指数数据：

```bash
uv run python -m a_share_research.sync_index_data --mode init
```

刷新指数数据：

```bash
uv run python -m a_share_research.sync_index_data --mode refresh
```

初始化当前 CSI 300 + CSI 500 成分股：

```bash
PYTHONPATH=src ./.venv/bin/python -m a_share_research.sync_stock_data --mode init
```

刷新成分股数据：

```bash
PYTHONPATH=src ./.venv/bin/python -m a_share_research.sync_stock_data --mode refresh
```

只补个股 60 分钟数据：

```bash
PYTHONPATH=src ./.venv/bin/python -m a_share_research.sync_stock_data --mode refresh --skip-universe --intraday-only
```

调试少量股票：

```bash
PYTHONPATH=src ./.venv/bin/python -m a_share_research.sync_stock_data --mode init --symbols 000001 600000 300750
```

## 页面入口

看板入口：

- `/`
- `/markets`

研究页：

- `/markets/indexes`
- `/markets/indexes/{symbol}`
- `/markets/stocks`
- `/markets/stocks/{symbol}`

兼容旧入口：

- `/instruments/{symbol}`

可直接从 Python 读取数据：

```python
from a_share_research import load_bar_1d, load_bar_60m, load_stock_bar_1d, load_stock_bar_60m

hs300_daily = load_bar_1d("000300")
pingan_daily = load_stock_bar_1d("000001")
pingan_60m = load_stock_bar_60m("000001")
```

## 目录结构

```text
src/a_share_research/
  domain/          资产类型等公共领域对象
  repositories/    数据库查询层
  services/        页面装配、symbol 解析、研究页逻辑
  web/             FastAPI app 与路由
  templates/       Jinja 页面、partials、components
  static/          CSS / JS
  index_data.py    指数元数据与抓取
  stock_data.py    个股元数据与抓取
  sync_*.py        数据同步入口
  db.py            兼容层与通用数据库能力
scripts/
  启停脚本与辅助命令
tests/
  看板与服务集成测试
data/
  原始与处理后的数据文件
notebooks/
  研究 notebook
reports/
  导出图表与报告
```
