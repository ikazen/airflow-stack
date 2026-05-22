# Troubleshooting

운영 중 실제로 부딪힌 문제 + 진단 흐름 + 해결 사례. **사전 가상 문제 안 적음.**

현 상태: **stub.** Phase 3 이후 발생 시 항목 추가.

## 작성 템플릿

각 항목 다음 구조로:

### [짧은 제목 — 한 줄 증상]

- **증상**: 정확한 에러 메시지 / 화면 / 발생 시점
- **진단**: 어떤 명령 / 로그로 좁혀 갔나 (재현 가능한 순서)
- **원인**: 진짜 원인 한 줄
- **해결**: 최종 조치 (명령 / 설정 변경 / 코드)
- **재발 방지**: runbook / 코드 / 알람 / 문서로 박았는가

## 점검 가이드 (Phase 진행 시)

옛 repo (로컬 archive) 의 troubleshooting 항목 중 새 환경에서도 유효 가능성 있는 것들 — 실제 발생 시 흡수:

- OCI ARM 위 docker image 의 wheel 호환성 (Apple silicon ARM 과 미묘하게 다름)
- iptables / Docker bridge ↔ OCI VCN 충돌
- Docker Compose v2 의 healthcheck / depends_on 의존성 순서
- Postgres bind address 와 컨테이너 네트워크 inter-op
- Caddy → upstream 의 long-poll timeout (Edge Executor 의 worker poll 길어질 때)
