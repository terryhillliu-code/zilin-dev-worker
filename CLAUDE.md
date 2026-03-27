# CLAUDE.md - 知微系统维护指南

## 核心命令 (Core Commands)
- **下发任务给大脑**: `~/zhiwei-dev/scripts/brain_cmd.sh "任务描述"`
- **重启 Bot**: `launchctl kickstart -k gui/$(id -u)/com.zhiwei.bot`
- **重启 Worker**: `launchctl kickstart -k gui/$(id -u)/com.zhiwei.dev-worker`
- **容器管理**: `docker restart clawdbot` / `docker logs -f clawdbot`

## 路径约定 (Paths)
- **日志**: `~/logs/`
- **共享库**: `~/zhiwei-common/` (修改后运行 `pip install -e .`)
- **宿主机代码**: `~/zhiwei-bot/`, `~/zhiwei-scheduler/`, `~/zhiwei-dev/`
- **容器内路径**: `/root/workspace/` (对应宿主机的特定 mount)
