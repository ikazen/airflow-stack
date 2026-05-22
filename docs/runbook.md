# Runbook

정상 흐름 운영 절차. 비정상 상황의 진단·해결은 `docs/troubleshooting.md`.

현 상태: **stub.** Phase 1 이후 실제 운영 시작하면 항목 추가.

## 작성 예정 항목

- 일상 헬스 체크 (api-server / scheduler / dag-processor / Postgres / Caddy / Edge Workers)
- 코드 배포 v1 절차 (push → 각 호스트 `git pull` → 영향 컨테이너 / launchd restart)
- 워커 재기동 (worker-vm container restart / M1 launchctl unload+load)
- Secrets 회전 (Fernet / JWT / Postgres pw)
- Postgres 백업 / 복원 (Bronze RPO/RTO + 메타 손실 시 재배포 시나리오)
- Caddy LE 인증서 갱신 검증
- Tailscale 노드 키 갱신 / ACL 변경 적용
