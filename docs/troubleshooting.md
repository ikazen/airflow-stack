# Troubleshooting

운영 중 실제로 부딪힌 문제 + 진단 흐름 + 해결 사례. 사전 가상 문제 안 적음.

각 항목: 증상 / 진단 명령·로그 / 원인 / 해결 / 재발 방지.

## 로그인 페이지 사라짐 (인증 bypass)

**증상**: UI 접근 시 로그인 없이 바로 진입. 세션 개념 자체가 사라진 것처럼 보임.

**원인**: `.env` 에 `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True` 설정 시 SimpleAuthManager 가 모든 요청을 인증 없이 익명 admin으로 처리. `GET /api/v2/dags` 등 모든 엔드포인트가 200 반환, JWT의 `sub` 가 `Anonymous` 로 찍힘.

**해결**: `.env` 에서 `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS` 줄 삭제 후 api-server recreate.
```bash
# ops-vm
sed -i '/AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS/d' infra/ops-vm/.env
docker compose -f infra/ops-vm/docker-compose.yml up -d api-server
```
복구 확인: `curl -s -o /dev/null -w '%{http_code}' http://<tailnet-ip>:8080/api/v2/dags` → `401`.

**재발 방지**: `all_admins=True` 는 dev/디버깅용. 운영 `.env` 에 절대 포함하지 말 것.

---

## mac-server sleep 후 task 좀비 + worker wedge

**증상**: mac sleep 중이던 task 가 깨어나도 UI 에서 영원히 `running`. 로그 안 뜸. worker 는 `No new job to process, N still running` 만 반복하며 새 job 안 받음.

**진단**:
```
docker logs mac-server-edge-worker-1            # "Signature has expired" (JWT), "N still running"
psql -d airflow -c "SELECT worker_name, state FROM edge_worker;"   # mac-server: running/unknown
psql -d airflow -c "SELECT task_id, state FROM edge_job WHERE edge_worker='mac-server';"
```

**원인**: sleep 중 clock 정지 → 깨어나면 task subprocess 의 JWT 만료 → 결과 보고 실패. worker 는 phantom 슬롯을 점유한 채 wedge.

**중앙(ops-vm) 자동 복구** (edge3 기본 동작, 추가 설정 불필요):
- worker `last_update` > `edge.heartbeat_interval`×5 (= 150s) → state `UNKNOWN` (`_check_worker_liveness`)
- RUNNING job `last_update` > `scheduler.task_instance_heartbeat_timeout` (= 300s, 기본) → TI 실제 상태로 reconcile (`_update_orphaned_jobs`) → retries 있으면 재시도

> 주의: Airflow 2.x 의 `scheduler_zombie_task_threshold` 는 3 에서 `task_instance_heartbeat_timeout` 로 개명. 옛 키·`task_heartbeat_sec` 는 효과 없음.

**해결 (wedge 된 worker)**: 재등록은 기존 row 가 `OFFLINE`/`UNKNOWN` 일 때만 허용 (그 외엔 409). 빠른 `docker restart` 연타는 `restart:unless-stopped` 와 경합해 `last_update` 를 계속 갱신 → 150s 를 못 채우는 self-inflicted crash loop. 올바른 복구: `up -d --force-recreate edge-worker` (idle/도달가능 worker 는 graceful SIGTERM 으로 deregister → 즉시 재등록).

**재발 방지**: wake 자동 복구(`sleepwatcher` + `~/.wakeup`) + 손실 치명적 DAG 의 `retries` — `setup.md` 참조.
