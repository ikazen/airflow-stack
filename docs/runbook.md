# Runbook

정상 흐름 운영 절차. 비정상 상황의 진단·해결은 `docs/troubleshooting.md`.

## mac-server (M1) colima 자동 시동 LaunchAgent

콜리마는 사용자 로그인 시 자동 시동, 죽으면 launchd 가 재기동. edge worker 컨테이너는 `restart: unless-stopped` 가 colima 위에서 자체 복귀.

plist: `~/Library/LaunchAgents/local.airflow.colima.plist`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>local.airflow.colima</string>
    <key>ProgramArguments</key>
    <array>
        <string>/opt/homebrew/bin/colima</string>
        <string>start</string>
        <string>-f</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>
    <key>RunAtLoad</key><true/>
    <key>KeepAlive</key><true/>
    <key>StandardOutPath</key><string>${HOME}/Library/Logs/colima.out.log</string>
    <key>StandardErrorPath</key><string>${HOME}/Library/Logs/colima.err.log</string>
</dict>
</plist>
```

설치: `launchctl load ~/Library/LaunchAgents/local.airflow.colima.plist`
중지: `launchctl unload ...` (foreground 모드라 colima 도 같이 죽음)

colima 자원 변경 (예: CPU 6 / mem 8) — 한번 `colima stop && colima start --cpu 6 --memory 8` 직접 실행, 설정은 `~/.colima/default/colima.yaml` 에 저장돼 이후엔 LaunchAgent 가 같은 값으로 자동 시동.

## 작성 예정 항목

- 일상 헬스 체크 (api-server / scheduler / dag-processor / Postgres / Caddy / Edge Workers)
- 코드 배포 v1 절차 (push → 각 호스트 `git pull` → 영향 컨테이너 / launchd restart)
- 워커 재기동 (worker-vm container restart / mac-server `launchctl unload+load` 또는 `docker compose restart`)
- Secrets 회전 (Fernet / JWT / Postgres pw)
- 인스턴스 / 메타 손실 시 재배포 복구 절차 (백업 없음 — decisions.md L10)
- Caddy LE 인증서 갱신 검증
- Tailscale 노드 키 갱신 / ACL 변경 적용
