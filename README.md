# TAO 监控系统

## 安装

```bash
cd /opt
git clone https://github.com/wuxiansheng8/taojk.git
cd /opt/taojk
./install.sh
```

## 更新

服务器上需要升级到 GitHub 最新版时执行：

```bash
cd /opt/taojk
./scripts/update.sh
```

`scripts/update.sh` 会丢弃项目目录里的本地代码改动，强制同步 GitHub 最新 `main` 分支，更新 Python 依赖，并重启 `taomonitor` 服务。`data.db`、`.env`、`venv` 等运行数据不在 Git 跟踪里，会保留。
