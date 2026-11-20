#!/bin/bash
# 在 runner（the coordinator node）上安装 fd-data-quality 的巡检与补齐任务。
# 为何用 docker：runner 自带 python3.8，脚本按 3.12 写；docker 保证版本一致且不污染宿主。
# 幂等：可重复执行（重建镜像、保留 .env，重写 cron）。
# 用法：sudo bash install_on_runner.sh [URL-ENCODED_PGPASS]
#
# NBS 多机 worker 不加入或合并任何 k3s 集群：每台 Tencent 机器独立构建/使用
# fd-data-quality:local，并以独立 Docker 容器运行。脚本只管理本机的 Docker、
# /opt/fd-data-quality 和 /etc/cron.d/fd-data-quality，不触碰 cheap 集群。
set -euo pipefail

APP_DIR=/opt/fd-data-quality
ENV_FILE=$APP_DIR/.env
CRON_FILE=/etc/cron.d/fd-data-quality
IMAGE=fd-data-quality:local
# 真实 host 写在这里而不是占位符：脱敏曾把它换成 `<db-host>`（2026-11-19 提交
# 7fd79b7），那让 `PG_HOST=<db-host>` 变成一次重定向尝试 —— 脚本在 parse 阶段就
# 语法错，根本无法执行（`bash -n` 报 `syntax error near unexpected token newline`）。
# 这是一个被脱敏改坏的活脚本，比「暴露一个内网 IP」严重得多。
PG_HOST="${FD_PG_HOST:-100.64.0.5}"
PGPASS_URL="${1:-}"
# PG 用户可配置：windows-admin 的 postgres 口令已不可考（2026-11-20），
# 而已知可用的是只读账号。回填需要写权限，所以必须显式指定有写权限的用户，
# 不要默认回 postgres——那会重演「口令对不上 → 只看到 UnicodeDecodeError」。
PG_USER="${FD_PG_USER:-postgres}"
PROXY_FORWARDER="${FD_PROXY_FORWARDER:-}"
OPEN_DATA_MCP_DATABASE_URL="${FD_OPEN_DATA_MCP_DATABASE_URL:-}"
PROXY_FETCH_LOG_DSN="${FD_PROXY_FETCH_LOG_DSN:-}"
WORKER_INDEX="${FD_NBS_WORKER_INDEX:-1}"
PULL_IMAGE="${FD_NBS_PULL_IMAGE:-0}"
NBS_CRON="${FD_NBS_CRON:-1}"

usage() {
  cat <<EOF
用法：
  sudo bash $0 [URL-ENCODED_PGPASS]
  FD_PROXY_FORWARDER=... FD_OPEN_DATA_MCP_DATABASE_URL=... sudo -E bash $0 [URL-ENCODED_PGPASS]

可选环境变量：
  FD_PROXY_FORWARDER             NBS 数据面 forwarder 根地址（必需用于 NBS proxy 模式；
                                 可以是本机 http://127.0.0.1:8080，也可以是共享的
                                 http://<forwarder-mesh-ip>:8080；不要填中央 :30090）
  FD_OPEN_DATA_MCP_DATABASE_URL  可选：master fd_open_data/fetch_log DSN
  FD_PROXY_FETCH_LOG_DSN         可选：优先于 FD_OPEN_DATA_MCP_DATABASE_URL 的 fetch_log DSN
  FD_NBS_WORKER_INDEX            当前机器 worker 编号（默认 1）
  FD_NBS_PULL_IMAGE              设为 1 时先 docker pull，再本地 build（默认 0）
  FD_NBS_CRON                    1=写 coordinator cron（runner 默认）；0=仅 worker 机器，不写 NBS coordinator cron
  FD_PG_USER                     PG 用户（默认 postgres）。**该 host 的 postgres 口令 2026-11-20
                                 已不可考**；若只有只读账号（如 fdbiz_ro）则回填会失败——
                                 回填需要 INSERT/UPDATE 权限，务必传有写权限的用户

打印当前机器的 worker 启动模板：
  FD_NBS_WORKER_INDEX=N bash $0 --print-worker-template
EOF
}

print_worker_template() {
  local index="${1:-1}"
  case "$index" in
    ''|*[!0-9]*)
      echo "FD_NBS_WORKER_INDEX 必须是正整数：$index" >&2
      return 2
      ;;
  esac

  # N 按机器唯一；例如 the coordinator node=1、chengsi=2，以此类推。
  printf '%s\n' \
    "docker run -d --restart unless-stopped \\" \
    "  --name fd-nbs-worker-${index} \\" \
    "  --network host \\" \
    "  -e TZ=Asia/Shanghai \\" \
    "  --env-file ${ENV_FILE} \\" \
    "  -v ${APP_DIR}/.state:/app/.state \\" \
    "  ${IMAGE} \\" \
    "  scripts/backfill_nbs.py --endpoint new --mode worker --min-interval 0"
}

if [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi
if [ "${1:-}" = "--print-worker-template" ]; then
  print_worker_template "${2:-$WORKER_INDEX}"
  exit 0
fi

upsert_env_value() {
  local key="$1"
  local value="$2"
  local tmp="${ENV_FILE}.tmp.$$"
  local found=0
  local line

  : > "$tmp"
  if [ -f "$ENV_FILE" ]; then
    while IFS= read -r line || [ -n "$line" ]; do
      case "$line" in
        "${key}"=*)
          printf '%s=%s\n' "$key" "$value" >> "$tmp"
          found=1
          ;;
        *)
          printf '%s\n' "$line" >> "$tmp"
          ;;
      esac
    done < "$ENV_FILE"
  fi
  if [ "$found" -eq 0 ]; then
    printf '%s=%s\n' "$key" "$value" >> "$tmp"
  fi
  mv "$tmp" "$ENV_FILE"
  chmod 600 "$ENV_FILE"
}

echo "== 1. 目录 =="
mkdir -p "$APP_DIR"/{.state/logs,baselines,scripts}

echo "== 2. 环境变量（DSN）=="
if [ -n "$PGPASS_URL" ] || [ ! -f "$ENV_FILE" ]; then
  if [ -z "$PGPASS_URL" ]; then
    echo "!! 首次安装必须传入 URL 编码后的口令（% -> %25, # -> %23）"; exit 2
  fi
  cat > "$ENV_FILE" <<EOF
FD_DQ_PG_URL=postgresql://${PG_USER}:${PGPASS_URL}@$PG_HOST:5432/dq
FD_WB_PG_URL=postgresql://${PG_USER}:${PGPASS_URL}@$PG_HOST:5432/world_bank
FD_CNSTATS_PG_URL=postgresql://${PG_USER}:${PGPASS_URL}@$PG_HOST:5432/cnstats
FD_GTA_PG_URL=postgresql://${PG_USER}:${PGPASS_URL}@$PG_HOST:5432/gta_panel
FD_CITY_PG_URL=postgresql://${PG_USER}:${PGPASS_URL}@$PG_HOST:5432/china_city_panel
FD_YEARBOOK_PG_URL=postgresql://${PG_USER}:${PGPASS_URL}@$PG_HOST:5432/yearbook_catalog
EOF
  chmod 600 "$ENV_FILE"
  echo "已写入 $ENV_FILE (600)"
else
  echo "保留已存在的 $ENV_FILE"
fi

# 这些值只在调用者显式提供时写入，避免把未配置的可选观测库变成空 DSN。
if [ -n "$PROXY_FORWARDER" ]; then
  upsert_env_value FD_PROXY_FORWARDER "$PROXY_FORWARDER"
  echo "已注入 FD_PROXY_FORWARDER（值不回显）"
else
  echo "警告：未设置 FD_PROXY_FORWARDER；NBS proxy worker 不会获得 forwarder 配置"
fi
if [ -n "$OPEN_DATA_MCP_DATABASE_URL" ]; then
  upsert_env_value FD_OPEN_DATA_MCP_DATABASE_URL "$OPEN_DATA_MCP_DATABASE_URL"
  echo "已注入 FD_OPEN_DATA_MCP_DATABASE_URL（值不回显）"
fi
if [ -n "$PROXY_FETCH_LOG_DSN" ]; then
  upsert_env_value FD_PROXY_FETCH_LOG_DSN "$PROXY_FETCH_LOG_DSN"
  echo "已注入 FD_PROXY_FETCH_LOG_DSN（值不回显）"
fi

echo "== 3. 构建镜像 =="
if [ "$PULL_IMAGE" = "1" ]; then
  docker pull "$IMAGE" || echo "警告：无法 pull $IMAGE，继续尝试本地 build"
fi
docker build -q -t "$IMAGE" "$APP_DIR" && echo "镜像就绪: $IMAGE"

echo "== 4. cron =="
if [ "$NBS_CRON" != "1" ]; then
  echo "FD_NBS_CRON=0：本机只运行 NBS worker，不写 coordinator cron"
else
cat > "$CRON_FILE" <<EOF
# fd-data-quality：巡检与补齐（runner 本地时区；docker 运行，env 从 $ENV_FILE 注入）
SHELL=/bin/bash
# 周一 03:30 World Bank 滚动刷新（最近 7 年：WDI 多数指标滞后 1-2 年，3 年窗覆盖不到）
30 3 * * 1 root cd $APP_DIR && (docker ps --format '{{.Names}}' | grep -q '^fd-wb-seed$' || docker run --rm --name fd-wb-cron -e TZ=Asia/Shanghai --env-file $ENV_FILE -v $APP_DIR/.state:/app/.state $IMAGE scripts/backfill_wb.py --mode rolling --years 7) >> $APP_DIR/.state/logs/wb_rolling.log 2>&1
# 每日 05:20 NBS coordinator 分配 hgnd/hgyd shard；各机器 worker 自动认领
20 5 * * * root cd $APP_DIR && (docker ps --format '{{.Names}}' | grep -q '^fd-nbs-coordinator$' || docker run --rm --network host --name fd-nbs-coordinator -e TZ=Asia/Shanghai --env-file $ENV_FILE -v $APP_DIR/.state:/app/.state $IMAGE scripts/backfill_nbs.py --endpoint new --mode coordinate --dbcode hgnd,hgyd --wait --wait-timeout 21600) >> $APP_DIR/.state/logs/nbs_coordinate.log 2>&1
# 周日 05:20 NBS 其余库 coordinator 入口（保留 weekly 调度）
20 5 * * 0 root cd $APP_DIR && (docker ps --format '{{.Names}}' | grep -q '^fd-nbs-coordinator-w$' || docker run --rm --network host --name fd-nbs-coordinator-w -e TZ=Asia/Shanghai --env-file $ENV_FILE -v $APP_DIR/.state:/app/.state $IMAGE scripts/backfill_nbs.py --endpoint new --mode coordinate --dbcode hgjd,fsnd,fsyd,fsjd,csnd --wait --wait-timeout 86400) >> $APP_DIR/.state/logs/nbs_coordinate_weekly.log 2>&1
# 周一 04:50 缺口归因：把 World Bank 停更缺口标注为"上游停发/可补齐"
50 4 * * 1 root cd $APP_DIR && docker run --rm -e TZ=Asia/Shanghai --env-file $ENV_FILE -v $APP_DIR/.state:/app/.state $IMAGE scripts/annotate_wb_upstream.py >> $APP_DIR/.state/logs/annotate_wb.log 2>&1
# 周一 06:40 覆盖度巡检（在补齐任务之后）
40 6 * * 1 root cd $APP_DIR && docker run --rm -e TZ=Asia/Shanghai --env-file $ENV_FILE -v $APP_DIR/.state:/app/.state $IMAGE scripts/coverage_scan.py --source all >> $APP_DIR/.state/logs/scan.log 2>&1
EOF
chmod 644 "$CRON_FILE"
echo "已写入 $CRON_FILE"
fi

echo "== 5. NBS worker 启动模板 =="
print_worker_template "$WORKER_INDEX"
echo
echo "请在每台 Tencent 机器上分别执行上面的模板；不要执行 k3s join/merge，也不要修改 cheap 集群。"

echo "== 6. 冒烟（容器内跑一次巡检 cnstats）=="
docker run --rm --env-file "$ENV_FILE" -v "$APP_DIR/.state:/app/.state" "$IMAGE" scripts/coverage_scan.py --source cnstats -v
echo "== 完成 =="
