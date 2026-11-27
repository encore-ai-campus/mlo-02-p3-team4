# fds — 탐지 백엔드

송금 요청을 두 단계로 검사하고, 경보를 사건으로 만들어 관제 웹에 넘기는 백엔드

---

## 목차

1. [개요](#1-개요)
   - [1-1 두 단계 검사](#1-1-두-단계-검사)
   - [1-2 전체 흐름](#1-2-전체-흐름)
   - [1-3 파일 구성](#1-3-파일-구성)
2. [1단계: 게이트웨이](#2-1단계-게이트웨이)
   - [2-1 검사 순서](#2-1-검사-순서)
   - [2-2 1단계 룰](#2-2-1단계-룰)
   - [2-3 저장 순서와 장애 처리](#2-3-저장-순서와-장애-처리)
3. [2단계: Spark](#3-2단계-spark)
   - [3-1 분석 시점](#3-1-분석-시점)
   - [3-2 2단계 룰](#3-2-2단계-룰)
   - [3-3 경보 전송 (빠짐·중복 방지)](#3-3-경보-전송)
   - [3-4 Spark 재계산](#3-4-spark-재계산)
4. [사건 처리: 경보 소비자](#4-사건-처리-경보-소비자)
   - [4-1 경보 → 사건 → 자동 동결](#4-1-경보--사건--자동-동결)
   - [4-2 사건 합치기](#4-2-사건-합치기)
   - [4-3 사건 상태](#4-3-사건-상태)
5. [API](#5-api)
   - [5-1 직원 API (조치)](#5-1-직원-api)
   - [5-2 관제 화면용 조회 API](#5-2-관제-화면용-조회-api)
6. [저장소](#6-저장소)
   - [6-1 Mongo](#6-1-mongo) · [6-2 Redis](#6-2-redis) · [6-3 Kafka](#6-3-kafka) · [6-4 ClickHouse](#6-4-clickhouse-장기-이력)
7. [설정](#7-설정)
   - [7-1 기간 설정](#7-1-기간-설정-period_profile)
   - [7-2 바꾸기 쉬운 값](#7-2-바꾸기-쉬운-값)
   - [7-3 실험·운영 스위치](#7-3-실험운영-스위치-환경-변수)
8. [운영](#8-운영)
   - [8-1 코드 반영](#8-1-코드-반영) · [8-2 운영 명령](#8-2-운영-명령) · [8-3 Kafka 토픽 만들기](#8-3-kafka-토픽-만들기) · [8-4 주의](#8-4-주의)
9. [부록: 11/23 통합 반영](#9-부록)

---

## 1. 개요

### 1-1 두 단계 검사

| 단계 | 어디서 | 무엇을 보나 | 결과 |
|---|---|---|---|
| 1단계 (즉시) | 게이트웨이 + Redis | 거래 시각 기준 최근 5분, R01~R07 | 3분 보류(429) 또는 차단(403) |
| 2단계 (장기) | Spark, 10초마다 | Mongo 최근 2주 기록, L1~L8 | 자동 동결 또는 검토 대기 사건 |

- 보류와 동결은 **송금만** 막는다. 보류·동결 지갑도 입금은 받는다.
- 1단계는 Redis만 보고 바로 답하고, 2단계는 Kafka 메시지로 "누구를 볼지" 고른 뒤 Mongo 기록으로 계산한다.

### 1-2 전체 흐름

```text
수집기/봇 → 게이트웨이 + Redis (1단계: 최근 5분, R01~R07)
  ├ 통과     → Mongo transfers + outbox → Publisher → ① transfer.events.v1 ──────┐
  ├ 보류     → Mongo fds_holds  + outbox → Publisher → ② fds.holds.v1 (HOLD) ────┤
  ├ R07 차단 → 받는 지갑이 블랙리스트일 때만     → ② fds.holds.v1 (BLOCK) ───────┤
  └ 모든 거절 → Mongo transfers(REJECTED) + outbox → transfer.rejected.v1 (ClickHouse만)
                                                                              ▼
                      Spark (2단계: 10초마다, Mongo 최근 2주 조회, L1~L8)
                            │ Mongo fds_alert_outbox에 먼저 저장 → ③ fds.alerts.v1
                            ▼
                      경보 소비자
                            ├ L1·L2·L3·L8 → 사건 저장 + Redis 동결 → ④ fds.freezes.v1 → Spark (L5)
                            └ L4·L5·L6·L7 → 사건 저장 (검토 대기)
                            ▼
관제 웹 → 직원 API (/api/v1/admin) → 동결(→ ④) / 이상 없음 / 동결 해제
```

- 게이트웨이와 Spark는 Kafka로 보낼 메시지를 Mongo에 **먼저** 적어 두고 보낸다 (outbox). Kafka가 잠시 멈춰도 메시지가 빠지지 않는다.
- R01 5번째 송금처럼 통과와 보류가 함께 생기면 ①과 ②가 둘 다 나간다.

### 1-3 파일 구성

**게이트웨이 이미지 (`app/`)** — 게이트웨이, Publisher, 경보 소비자가 같은 이미지를 쓰고 실행 명령만 다르다.

| 파일 | 컨테이너 | 역할 |
|---|---|---|
| `app/main.py` | `aegis-gateway` | 송금 API: 요청 검사 순서, 응답 |
| `app/holds.py` | `aegis-gateway` | 1단계 R01~R06 판단 (R05 + Redis Lua), 3분 보류 |
| `app/wallet_status.py` | 게이트웨이, 소비자 | 동결, R07 블랙리스트, 보류 중 확인. 2단계 위험도 `RISK_LEVELS` |
| `app/whitelist.py` | `aegis-gateway` | 룰별 면제 확인 |
| `app/idempotency.py` | `aegis-gateway` | 중복 요청 확인 (Redis) |
| `app/store.py` | `aegis-gateway` | Mongo 저장 (거래, 보류 기록, outbox), 인덱스 |
| `app/admin.py` | `aegis-gateway` | 직원 API: 동결·해제, 사건 처리, 블랙리스트·화이트리스트, 지갑 상태 |
| `app/admin_views.py` | `aegis-gateway` | 관제 화면 조회 API (사건·통계·거래·감사·상태·검색), 화면 조치 버튼 어댑터 |
| `app/live_alerts.py` | `aegis-gateway` | 실시간 경보 WebSocket `/ws/admin` (Mongo change stream) |
| `app/str_draft.py` | `aegis-gateway` | STR(의심거래보고) 초안 조회, 분석관 의견 저장 |
| `app/eval_poc.py` | `aegis-gateway` | [EVAL] 봇 정답 라벨 → 룰별 성적표 (`EVAL_ENABLED=0`이면 꺼짐) |
| `app/mongo_client.py`, `app/redis_client.py` | 게이트웨이 | Mongo·Redis 연결 |
| `app/publisher.py` | `aegis-publisher` | `outbox_events`를 문서의 `topic`대로 Kafka에 전송 |
| `app/alert_consumer.py` | `aegis-alert-consumer` | L1~L8 경보 → 사건 저장, 자동 동결, 동결 이벤트 전송 |

**Spark 이미지 (`spark/`)**

| 파일 | 역할 |
|---|---|
| `spark/stream.py` | Kafka 토픽 3개 읽기, 배치마다 경보 저장 → 전송, 하트비트 |
| `spark/detector.py` | 배치 흐름: 계기 ①②③, 분석할 지갑 고르기, 제외. 계산(`analyze`)과 처리 표시(`mark_done`)를 나눔 |
| `spark/queries.py` | Mongo 조회 (여러 지갑을 한 번에, L2·L3·L6과 L4·L8은 조회를 같이 씀) |
| `spark/rules.py` | L1~L8 판단 (조회 결과 → 경보, Mongo 없이 테스트 가능) |
| `spark/crosscheck.py` | L2·L6·L7 경보의 원본 거래를 Spark로 다시 계산 (1분 내역, 5분 이동 윈도, 판정 값 대조) |
| `spark/alert_outbox.py` | 경보 전송 대기함 `fds_alert_outbox` (저장, 못 보낸 경보 다시 보내기) |
| `spark/config.py` | 기간 프로필(prod/demo), L1~L8 기준값, 처리량 제한 |

**공용 · 기타**

| 파일 | 쓰는 곳 | 역할 |
|---|---|---|
| `common/keys.py` | 전부 | 토픽 이름, Redis 키, R05 기준 |
| `common/entities.py`, `common/entities.json` | 게이트웨이 | 알려진 주소 이름표 (거래소·DEX 등). 관제 화면에 이름으로 표시 |
| `clickhouse/*.sql`, `clickhouse/kafka.xml` | `aegis-clickhouse` | 장기 이력 테이블 4개와 Kafka 구독 설정 ([6-4 ClickHouse](#6-4-clickhouse-장기-이력)) |

- 컨테이너 종류마다 폴더 하나에 `Dockerfile`과 `requirements.txt`를 함께 둔다.
- `common/`은 게이트웨이 이미지와 Spark 이미지에 복사된다 (둘 다 빌드 위치 `fds/`).
- Spark 이미지에는 `spark/`·`common/` 코드가 들어 있어 k3s는 이미지 그대로 실행하고, 로컬 compose는 같은 폴더를 볼륨으로 덮어써서 재시작만으로 코드가 반영된다.
- Spark 이미지에 넣을 파일은 `spark/Dockerfile.dockerignore`가 정한다 (`fds/.dockerignore`는 게이트웨이용).

---

## 2. 1단계: 게이트웨이

### 2-1 검사 순서

요청: `POST /api/v1/transfers`, 헤더 `X-API-Key` 필요.
형식이 틀린 요청은 FastAPI가 먼저 422로 돌려보내며 저장하지 않는다. `exchange_name`, `log_index`는 선택 필드다.

| 순서 | 검사 | 실패 응답 | Spark에 알림 |
|---|---|---|---|
| 1 | API 키 | 401 `UNAUTHORIZED` (저장 안 함) | - |
| 2 | 금액: 0보다 크고 소수점 6자리 이하 | 422 `INVALID_AMOUNT` | - |
| 3 | 시각: 서버 시각과 30초 이내 | 422 `TIMESTAMP_OUT_OF_RANGE` | - |
| 4 | 보내는 지갑 동결 | 403 `FROZEN_L1` ~ `FROZEN_L8`, `FROZEN_MANUAL` | - |
| 5 | R07 블랙리스트 (보내는 지갑 / 받는 지갑) | 403 `BLACKLISTED_SENDER` / `BLACKLISTED_RECEIVER` | 받는 지갑일 때만 ② |
| 6 | 보내는 지갑이 보류 중 | 429 `TEMP_HOLD_Rxx`, `Retry-After: 남은 초` | - |
| 7 | 중복: `tx_hash` + `log_index` | 409 `DUPLICATE_REQUEST` | - |
| 8 | R05: 100,000 USDT 이상 | 429 `TEMP_HOLD_R05` + 보내는 지갑 3분 보류 | ② |
| 9 | R02, R04 | 429 `TEMP_HOLD_Rxx` + 보내는 지갑 3분 보류 | ② |
| 10 | R01 (5분 안 5번째 송금) | **200 통과** + 보내는 지갑 3분 보류. 6번째부터는 6번 검사(보류 중)에서 429 | ① + ② |
| 11 | R03, R06 | **200 통과** + 받는 지갑 3분 보류 | ① + ② |
| - | Redis, Mongo 장애 | 503 `FDS_UNAVAILABLE` | - |

- 통과하면 200 `{"status":"ACCEPTED","reason":null}`. R01로 보류가 걸리면 `hold_applied`, `hold_rule`, `hold_until`이 함께 온다.
- 429는 남은 보류 초를 헤더 `Retry-After`와 본문 `retry_after`에 같이 준다.
- 거절 기록은 `transfers`에 `status: REJECTED`로 남고, 같은 내용이 `transfer.rejected.v1`로도 간다 (401은 저장도 전송도 안 함).
- 중복 검사를 R룰 계산 앞에 둬서, 재전송한 거래가 개수에 두 번 들어가지 않는다.
- 429로 거절된 거래는 중복 키를 풀어 두므로, 보류가 끝난 뒤 같은 `tx_hash`로 다시 보낼 수 있다.
- 같은 거래가 여러 룰에 걸리면 첫 룰(R01 → R02 → R04 순)이 사유가 되고, 전체 목록은 `fds_holds.rules`에 남는다.

### 2-2 1단계 룰

거래 시각 기준 최근 5분을 정확히 계산한다 (Redis Sorted Set, 점수 = 거래 시각).

| 룰 | 이름 | 기준 | 보류 대상 | 이 거래 |
|---|---|---|---|---|
| R01 | 초고빈도 송금 | 송금 5건 이상 | 보내는 지갑 | 5번째는 통과, 보류 중 6번째부터 거절 |
| R02 | 쪼개기 송금 | 900~990 USDT 송금 3회 이상 | 보내는 지갑 | 거절 |
| R03 | 자금 집금 | 서로 다른 보낸 지갑 5개 이상 | 받는 지갑 | 통과 |
| R04 | 자금 분산 | 서로 다른 받는 지갑 5개 이상 | 보내는 지갑 | 거절 |
| R05 | 대량 송금 | 1건 100,000 USDT 이상 | 보내는 지갑 | 거절 |
| R06 | 핑퐁 | A→B 뒤 5분 안에 B→A | 먼저 보낸 지갑(A) | 통과 |
| R07 | 블랙리스트 | 보내거나 받는 지갑이 블랙리스트 | 보류 없이 항상 차단 | 거절 |

- 거절된 거래는 5분 개수에 넣지 않는다. 걸린 그 거래도 포함하지 않는다.
- 화이트리스트 지갑(`fds:whitelist:R01` ~ `R06`)은 해당 룰로 보류하지 않는다. R07은 면제가 없다.
- 이미 보류 중이거나 동결된 받는 지갑은 다시 보류하지 않는다.
- 자기 자신에게 보내는 송금(보낸 주소 = 받는 주소)은 R03·R06을 보지 않는다.
- 보류를 거는 순간 그 룰의 5분 기록을 지운다: R01(5번째)은 `fds:w:send`, R02는 `fds:w:split`, R03은 `fds:w:in`. 보류가 풀리자마자 남은 기록으로 다시 걸리지 않게 하기 위해서다. R04·R06은 `fds:w:out`을 같이 써서 지우지 않는다.

### 2-3 저장 순서와 장애 처리

| 결과 | 저장 시점 | 저장이 실패하면 |
|---|---|---|
| 통과 (보류 기록 포함) | `transfers` + `outbox_events` (+ `fds_holds`)를 트랜잭션 하나로 저장하고, **커밋이 끝난 뒤** 200 | 503. 중복 키를 풀고 Redis 5분 기록(`send`·`split`)과 받는 지갑 보류를 되돌린다 |
| 보내는 지갑 보류 (429) | 거절 기록 + 보류 기록 + 이벤트를 **응답 전에** 저장 (L1이 이 보류 기록을 센다) | Redis 보류는 그대로 걸려 있고, 로그만 남긴다 |
| 그 밖의 거절 | 응답을 먼저 보내고, 그다음 저장 | 로그만 남긴다 |

- Redis 중복 키가 사라져도 Mongo 고유 인덱스(`tx_hash` + `log_index`, 성공 건)가 같은 거래가 두 번 저장되는 것을 한 번 더 막는다 (409).
- Redis·Mongo가 응답하지 않으면 검사 없이 통과시키지 않고 503 `FDS_UNAVAILABLE`을 돌려준다.
- 게이트웨이 로그는 `logs/gateway.log`에도 남는다.

---

## 3. 2단계: Spark

### 3-1 분석 시점

| 계기 | 토픽 | 분석 대상 | 시점 |
|---|---|---|---|
| ① 보류 | `fds.holds.v1` (HOLD) | 보류된 지갑 | 다음 배치 (10초 안) |
| ② 거래 | `transfer.events.v1`, `fds.holds.v1` (R07 BLOCK) | 보낸 지갑, 받은 지갑 | 다음 배치. 단, 재분석 간격 안이면 대기 목록으로 미룸 |
| ③ 동결, 블랙리스트 | `fds.freezes.v1`, Redis 블랙리스트 비교 | 그 지갑과 최근 2주 안에 거래한 상대 지갑 (L5만) | 배치마다 500개씩 |

- 토픽 3개를 쿼리 하나로 10초마다 읽는다. 보류는 어차피 3분이라 10초면 충분하다.
- 거래가 없는 동안에도 대기 목록을 처리하도록 1초에 1줄씩 시계 신호(`rate`)를 같이 읽는다.
- 한 배치에서 전체 분석하는 지갑은 **최대 2,000개**다. 순서는 보류(①) → 대기 목록 → 새 거래이고, 넘친 지갑은 대기 목록으로 넘겨 다음 배치에서 본다.
- 블랙리스트는 직원 API로 등록하면 Redis에 바로 들어간다. Spark는 배치마다 `fds:blacklist_seen`과 비교해 새로 등록된 주소를 찾는다.
- 화이트리스트 `fds:whitelist:L`, 동결, 블랙리스트 지갑은 분석하지 않는다.
- 한 배치에서 Mongo 조회는 룰 묶음마다 1번씩이다. L1·L2·L3·L5·L6·L7은 Mongo가 지갑별 숫자만 돌려주고, L4·L8은 거래 원본을 받아 Spark가 순서대로 짝짓는다.
- 배치가 끝날 때마다 Redis `fds:heartbeat:spark`에 시각을 남긴다. 관제 화면 시스템 상태가 이 값으로 Spark가 살아 있는지 본다.

### 3-2 2단계 룰

| 룰 | 이름 | 기준 (prod 기간) | 처리 |
|---|---|---|---|
| L1 | 반복 보류 | 2주 안에 1단계 보류 3회 이상 (`fds_holds`, 기본은 보낸 쪽 보류만 셈 `L1_HOLD_SIDE=sender`) | 자동 동결 |
| L2 | 쪼개서 대량 송금 | 24시간 누적 송금 100,000 이상, 모든 거래 100,000 미만 | 자동 동결 |
| L3 | 장기 쪼개기 | 2주 안에 900~990 송금 10회 이상 | 자동 동결 |
| L4 | 경유 지갑 | 1,000 이상 받고 1시간 안에 80% 이상 다시 송금, 2주 안에 3회 이상 | 검토 대기 |
| L5 | 위험 지갑 접촉 | 아래 등급표 | 검토 대기 |
| L6 | 급격한 활동 증가 | 최근 24시간 송금 수가 그 전 하루 평균의 10배 이상, 최소 20건 | 검토 대기 |
| L7 | 새 상대 집중 | 24시간 안에 처음 보내는 상대 20개 이상 (2주 기록 기준) | 검토 대기 |
| L8 | 순환 거래 | A→B→A 또는 A→B→C→A, 24시간 안에 시간 순서대로, 각 1,000 이상, 처음 금액의 80% 이상 귀환 | 자동 동결 |

**L5 등급** (통과된 거래만 셈, R07로 막힌 시도는 제외)

| 등급 | 조건 |
|---|---|
| CRITICAL | 블랙리스트 지갑에게서 직접 받음 |
| HIGH | 블랙리스트 지갑에게 보냄 |
| MEDIUM | 동결 지갑과 2주 누적 10,000 USDT 이상 주고받음 |

- 블랙리스트에 오른 뒤의 거래는 R07이 막으므로, CRITICAL·HIGH는 **블랙리스트 등록 전 2주 안의 거래**에서 찾는다 (계기 ③).
- MEDIUM의 "동결 지갑"은 기본적으로 모든 동결(자동 동결 포함)이다. `L5_EXPOSURE=confirmed`면 직원이 확인한 동결(`fds:frozen_confirmed`)만 쓴다.
- 자기 자신에게 보내는 송금은 L4·L8에서 빠진다.
- L6은 평소 기록이 없는 새 지갑은 보지 않는다.
- L8은 화이트리스트(`L`) 지갑과 상대가 200개 넘는 지갑을 거치는 경로는 따라가지 않는다.
- 직원이 동결 해제한 지갑은 **해제 시각 이후 기록만** 다시 분석한다 (`fds:unfrozen_at`). 해제 전 거래로 바로 다시 동결되지 않게 하기 위해서다. L7의 '처음 보낸 상대'는 해제 전 기록까지 보고 판단하고, L6은 해제 후 이틀이 지나야 본다.

### 3-3 경보 전송

게이트웨이 outbox와 같은 원리로, 경보를 Kafka로 보내기 전에 Mongo `fds_alert_outbox`에 먼저 적어 둔다.

```text
배치마다: 계산(analyze) → Spark 재계산 → fds_alert_outbox 저장(PENDING) → Redis 처리 표시(mark_done)
          → 못 보낸 경보 전부(이번 배치 + 지난 실패분) fds.alerts.v1 전송 → PUBLISHED
```

| 상황 | 결과 |
|---|---|
| Kafka 전송 실패, Spark 재시작 | 저장해 둔 PENDING 경보를 다음 배치에서 다시 보낸다. 재분석 간격 때문에 같은 경보가 다시 계산되지 않아도 빠지지 않는다 |
| 같은 배치를 다시 돌림 | 경보 번호 `alert_id` = `실행ID:배치번호:룰:지갑`이 같아서 두 줄로 저장되지 않는다 |
| 체크포인트를 새로 잡음 (`CHECKPOINT_DIR` 변경, 볼륨 삭제) | 배치 번호는 0부터 다시 시작하지만 실행 ID가 바뀌어서 예전 경보 번호와 겹치지 않는다 (겹치면 새 경보가 '이미 보낸 경보'로 버려짐) |
| 보낸 뒤 PUBLISHED 표시 전에 죽음 | 같은 경보가 한 번 더 나간다. 경보 소비자가 사건의 `alert_ids`로 이미 반영한 번호를 알아보고 사건은 건드리지 않는다 (동결 여부만 이어서 확인) |

- 처리 표시(재분석 간격, 대기 목록 정리)는 경보를 저장한 **뒤에** 한다. 먼저 하면 전송 실패 뒤 다시 돌릴 때 재분석 간격에 막혀 경보가 사라진다.
- 계기 ③의 '이미 본 동결·블랙리스트 주소' 표시도 L5 대기 목록에 넣은 뒤에 남긴다.
- 실행 ID는 Spark 쿼리 id(체크포인트 `metadata` 파일에 저장) 앞 8자리다. 같은 체크포인트로 재시작하면 그대로라 위 중복 방지가 유지된다. Spark 첫 배치 로그 `경보 번호 실행 ID=...`로 확인한다.
- 보낸 경보는 7일 뒤 자동 삭제. PENDING은 지워지지 않는다.
- 11/26 로컬 확인: 정상 흐름, 같은 경보 재발송, Spark 중지 중 쌓인 경보, Kafka 약 50초 중지 중 쌓인 경보 모두 사건·감사 원장·동결이 하나씩만 남음.

### 3-4 Spark 재계산

L2·L6·L7 경보는 판정은 바꾸지 않고, 경보 지갑의 원본 거래(`transfers` 승인 송금)를 Spark DataFrame으로 다시 계산해 근거로 붙인다. 사건 `recompute`에 저장되고 사건 상세·STR 초안 4-1이 보여 준다.

| 항목 | 내용 |
|---|---|
| 판정 값 대조 | L2 누적·최대·건수, L6 오늘·평소, L7 처음 보낸 상대 → `MATCH` / `MISMATCH` |
| 범위 요약 | 송금 횟수·합계·서로 다른 상대 수 |
| 1분 내역 | 1분마다 건수·합계·최대·상대 수 (`minutes`) |
| 5분 이동 윈도 | Spark `window(시각, "5 minutes", "1 minute")`로 5분 구간(1분 간격)마다 송금 횟수·금액·서로 다른 상대 수. 가장 많이 보낸 구간(`peak_count`), 상대가 가장 많았던 구간(`peak_receivers`), 최근 60개 구간 (`windows_5m`) |

- 상시 스트리밍 집계(워터마크·상태 저장)가 아니라, 경보가 뜬 지갑만 경보 시점에 원본으로 계산한다. 모든 주소를 계속 집계하는 것보다 비용이 작고 판정 근거와 같은 범위를 본다.
- 범위 안 거래만 세므로, 범위 처음·끝에 걸친 5분 구간은 일부 시간만 담긴다.
- `CROSSCHECK_ENABLED=0`이면 재계산을 끈다.

---

## 4. 사건 처리: 경보 소비자

`app/alert_consumer.py`가 `fds.alerts.v1`을 읽어 Mongo `fds_cases`에 사건을 만들고, 자동 동결 룰이면 지갑을 동결한다.

### 4-1 경보 → 사건 → 자동 동결

```text
경보 받음 → 버릴 경보인지 확인 → 이미 반영한 경보 번호인지 확인 → 사건 저장(새로 만들거나 합침)
         → L1·L2·L3·L8이면: 동결 이벤트(④) → 감사 원장 한 줄 → Redis 동결
```

| 경우 | 처리 |
|---|---|
| 화이트리스트 `L` 지갑의 경보 | 저장도 동결도 하지 않는다 |
| 동결 해제 시각보다 앞선 거래가 들어간 경보 | 버린다 (해제 직전에 계산돼 늦게 도착한 경보) |
| 이미 반영한 경보 번호 (`alert_ids`에 있음) | 사건은 건드리지 않고, 동결만 이어서 확인한다 |
| 이미 동결된 지갑 | 사건만 저장하고, 동결 이벤트·감사 기록은 다시 만들지 않는다 |
| Mongo·Redis·Kafka 오류 | 3초 쉬고 같은 경보를 계속 다시 처리한다. 저장소가 복구될 때까지 다음 경보로 넘어가지 않는다 |
| 형식이 틀린 경보 (필수 필드 없음, 모르는 룰) | 로그만 남기고 건너뛴다 |

- 사건 저장을 먼저, 동결을 그다음에 한다. 중간에 죽어서 다시 처리해도 같은 결과가 된다.
- 자동 동결 감사 기록은 `decision_id = SYSTEM:FREEZE:{사건 id}`라 몇 번 다시 처리해도 한 줄만 남는다.
- 동결 사유(`fds:frozen_reasons`)는 처음 동결한 룰을 유지한다.
- `L1_ACTION=review`면 L1을 자동 동결하지 않고 검토 대기로 둔다.

### 4-2 사건 합치기

같은 룰 + 같은 지갑의 경보는 사건 하나로 합친다. 거래 시각 기준으로 10분(`CASE_GAP`) 넘게 끊기면 새 사건을 만든다. 동결 해제된 사건(`UNFROZEN`)에는 합치지 않는다.

| 항목 | 합치는 방법 |
|---|---|
| 위험도 | 더 높은 쪽을 유지한다 (CRITICAL 뒤에 MEDIUM이 와도 CRITICAL) |
| 증거 `evidence` | 숫자는 최댓값, 목록은 합집합, 나머지는 새 값 |
| 거래 해시, 기간 | 합집합, 처음·마지막 거래 시각을 넓힌다 |
| Spark 재계산이 붙은 경보 (L2·L6·L7) | 증거를 섞지 않고 최신 경보 것으로 **통째로 바꾼다**. 바뀌기 전 근거는 `evidence_history`에 남긴다 (최근 20개) |
| 경보 번호 `alert_ids` | 최근 200개 |

- 재계산이 붙은 경보를 숫자별 최댓값으로 섞으면 "합계는 A 시점, 최대는 B 시점"처럼 실제로 없던 조합이 남아 재계산 값과 맞지 않게 된다. 그래서 통째로 바꾼다.

### 4-3 사건 상태

| `review_status` | 뜻 | 관제 화면 상태 |
|---|---|---|
| `null` (`action_taken: WALLET_FROZEN`) | 자동 동결 | `AUTO_FROZEN` |
| `PENDING` (`action_taken: REVIEW_PENDING`) | 검토 대기 | `PENDING` |
| `FROZEN` | 직원 동결 | `FROZEN` |
| `DISMISSED` | 이상 없음 | `DISMISSED` |
| `UNFROZEN` | 동결 해제 | `RELEASED` |

- 관제 화면 조회 API는 오른쪽 열 값으로 바꿔서 보낸다.
- 처리한 직원과 시각은 `reviewed_by`, `reviewed_at`, `review_rationale`(해제는 `unfrozen_by`, `unfrozen_at`, `unfreeze_rationale`)에 남는다.
- 케이스 없이 직원이 동결한 지갑은 `rule_id: MANUAL` 사건으로 남는다.
- STR 초안 관련 필드: `str_status`(`NOT_GENERATED` → 의견 저장 시 `DRAFTED`), `analyst_opinion`(분석관 의견). STR 초안 자체는 볼 때마다 사건 기록으로 새로 만들고 저장하지 않는다.

---

## 5. API

두 API 모두 직원 키를 쓴다. 브라우저 코드에는 키가 없고, 개발 서버(Vite 프록시)나 배포 Nginx가 헤더 `X-Admin-Key`를 붙여 넘긴다 (`.env`의 `ADMIN_API_KEY`, 송금 키와 다름). 키를 설정하지 않으면 503 `ADMIN_API_DISABLED`.

### 5-1 직원 API

조치(동결·해제·리스트 관리)를 한다. 주소는 전부 `/api/v1/admin`으로 시작한다 (`app/admin.py`).

| 기능 | 요청 | 하는 일 |
|---|---|---|
| 사건 보고 동결 | `POST /cases/{사건 _id}/freeze` | 사건 지갑 동결 (사유 = 사건 룰 → `FROZEN_L5` 등), `fds.freezes.v1`에 `FREEZE`(`source: MANUAL`) |
| 이상 없음 | `POST /cases/{사건 _id}/dismiss` | 검토 대기 사건만 닫음 (동결 안 함) |
| 사건 없이 동결 | `POST /wallets/{지갑}/freeze` | 신고·수사 요청 등으로 바로 동결 (사유 `FROZEN_MANUAL`). 기록용 `MANUAL` 사건을 만든다 |
| 동결 해제 | `POST /wallets/{지갑}/unfreeze` | 동결 목록·사유에서 빼고 해제 시각 기록, 그 지갑의 동결 사건을 모두 `UNFROZEN`으로, `UNFREEZE` 이벤트 |
| 지갑 상태 | `GET /wallets/{지갑}` | 동결 여부·사유, 블랙리스트, 화이트리스트 룰, 보류 중(남은 초), 해제 시각 |
| 블랙리스트 등록 | `POST /blacklist` `{"wallets": [...]}` | 바로 R07 차단. Spark가 다음 배치에서 상대 지갑 L5 재계산 (계기 ③). 최대 1,000개 |
| 블랙리스트 해제 | `POST /blacklist/{지갑}/remove` | R07 차단 해제. 이미 만들어진 L5 사건은 그대로 |
| 블랙리스트 목록 | `GET /blacklist?q=&page=1&size=50&history=false` | 등록 중인 주소 (`q`: 주소 일부, `history=true`: 해제 기록까지) |
| 화이트리스트 등록 | `POST /whitelist` `{"wallet": "...", "rules": ["R01", "L"]}` | 고른 룰만 면제 |
| 화이트리스트 해제 | `POST /whitelist/{지갑}/remove` `{"rules": [...]}` | 고른 룰 면제만 해제 |
| 화이트리스트 목록 | `GET /whitelist?q=&rule=L&page=1&size=50&history=false` | 등록 중인 (지갑, 룰) 목록 |

- POST 본문에는 공통으로 `"operator": "직원ID"`, `"rationale": "사유"`가 들어간다. 사유는 필수이고(없으면 422) 감사 원장 `policy_decisions`에 남는다.
- 성공하면 200 `{"status":"OK", ...}`, 실패하면 `{"status":"ERROR","reason":...}`.
- 블랙리스트·화이트리스트 등록은 `added`(새로 등록)와 `already`(이미 등록돼 있던 것)를, 화이트리스트 해제는 `removed`와 `not_listed`를 돌려준다.
- 직원 동결·사건 없이 동결·이미 동결된 지갑의 직원 확인은 Redis `fds:frozen_confirmed`(직원이 확인한 동결)에도 넣고, 해제하면 뺀다. 자동 동결은 넣지 않는다.

| 응답 | 뜻 |
|---|---|
| 200 `FROZEN` / `ALREADY_FROZEN` | 동결함 / 이미 다른 룰로 동결돼 있어 직원 판단만 기록 |
| 200 `DISMISSED` / `UNFROZEN` | 이상 없음 처리 / 동결 해제 (`cases_updated`: 해제로 바뀐 사건 수) |
| 401 `UNAUTHORIZED` | 직원 키가 틀림 |
| 404 `CASE_NOT_FOUND` | 사건 없음 |
| 409 `NOT_PENDING` | 검토 대기가 아닌 사건에 이상 없음 (동결된 사건은 동결 해제로) |
| 409 `NOT_FROZEN` / `CONFLICT` | 동결되지 않은 지갑 해제 / 다른 직원이 먼저 처리함 |
| 409 `NOT_LISTED` | 등록돼 있지 않은 블랙리스트·화이트리스트 해제 |
| 422 | 본문 형식 오류 (operator 없음, 사유 없음, 모르는 룰, 화이트리스트 메모 없음 등) |
| 503 `FDS_UNAVAILABLE` | Redis·Mongo 장애. 같은 요청을 다시 보내면 남은 부분만 처리된다 |

### 5-2 관제 화면용 조회 API

관제 웹 화면이 읽는 API다. 조치는 위 직원 API가 하고, 여기는 조회만 한다 (`app/admin_views.py`).

| 요청 | 내용 | 코드 |
|---|---|---|
| `GET /cases?limit=200` | 경보(사건) 목록 | `admin_views.py` |
| `GET /cases/{id}` | 사건 상세 (근거·관련 거래·자금 요약·Spark 재계산) | `admin_views.py` |
| `GET /cases/{id}/graph` | 자금 흐름 그래프 | `admin_views.py` |
| `GET /stats?range=24h` | 대시보드 집계 (KPI·시계열·룰별·위험 지갑 상위) | `admin_views.py` |
| `GET /transfers?limit=300` | 실시간 거래 (판정 결과 포함) | `admin_views.py` |
| `GET /audit` | 감사 이력 (사건 생성 + 감사 원장) | `admin_views.py` |
| `GET /health` | 시스템 상태와 데이터 신뢰성 (원장 신선도, outbox 대기, Mongo ↔ ClickHouse 적재 대조, Spark 하트비트) | `admin_views.py` |
| `GET /search?q=` | 지갑 주소·사건 ID·tx_hash 검색 (3자 이상, 최대 8건) | `admin_views.py` |
| `POST /cases/{id}/decision` | 화면 조치 버튼 어댑터 (아래 표) | `admin_views.py` |
| `GET`·`PATCH /cases/{id}/str-draft` | STR 초안 조회, 분석관 의견 저장 | `str_draft.py` |
| `WS /ws/admin` | 새 사건(`case_created`)·사건 변경(`case_updated`) 실시간 알림. 헤더 `X-Admin-Key` | `live_alerts.py` |
| `GET /eval/score` | [EVAL] 룰별 성적표. 봇은 `POST /api/v1/eval/labels`로 정답을 보냄 | `eval_poc.py` |

**화면 조치 버튼** (`POST /cases/{id}/decision`, 본문 `note` → 직원 API `rationale`. 비어 있으면 `관제 화면 조치 (사유 미입력)`으로 기록)

| 버튼 `action` | 넘기는 곳 | 비고 |
|---|---|---|
| `FREEZE` | `POST /cases/{id}/freeze` | 검토 대기 사건 → 직원 동결 |
| `DISMISS` | `POST /cases/{id}/dismiss` | 검토 대기만 가능. 동결된 사건은 409 `NOT_PENDING` → `RELEASE` 사용 |
| `RELEASE` | `POST /wallets/{지갑}/unfreeze` | **지갑 단위**라 그 지갑의 동결 사건이 모두 해제된다 |
| `STR_DRAFT` | - | 상태 변화 없이 감사 원장에 한 줄만 남긴다 |

- `WS /ws/admin`을 뺀 나머지는 모두 `/api/v1/admin`으로 시작한다 (예: `/api/v1/admin/cases`). 정답 라벨 등록(`/api/v1/eval/labels`)만 송금 키를 쓴다.
- 상태 확인: `GET /healthz`(프로세스 생존), `GET /ready`(Redis·Mongo 연결). k3s 프로브와 compose healthcheck가 쓴다.

---

## 6. 저장소

### 6-1 Mongo

| 이름 | 내용 |
|---|---|
| `transfers` | 모든 요청 원본 (통과, 거절). 자동 삭제는 `TRANSFERS_TTL_DAYS`로 켬 (지금 0 = 끔) |
| `fds_holds` | 새로 걸린 보류 (지갑, 룰, 보낸/받은 쪽, 근거). 14일 뒤 자동 삭제 |
| `outbox_events` | 게이트웨이 → Kafka 전송 대기함. 전송 후 24시간 뒤 자동 삭제 |
| `fds_alert_outbox` | Spark 경보 전송 대기함 (`_id` = 경보 번호, `PENDING` → `PUBLISHED`). 전송 후 7일 뒤 자동 삭제 |
| `fds_cases` | L1~L8 사건 (상태는 [4-3](#4-3-사건-상태)). 증거, 기준값 `criteria`, 계산 출처 `source`, 재계산 `recompute`, 이전 근거 `evidence_history`, 반영한 경보 번호 `alert_ids`, STR 필드 |
| `policy_decisions` | 감사 원장. 자동 동결(SYSTEM)·직원 조치(ANALYST)·리스트 변경·STR 저장을 한 줄씩 쌓기만 함 (`decision_id` 고유) |
| `fds_blacklist`, `fds_whitelist` | 블랙리스트·화이트리스트 기록 (등록 한 번 = 문서 하나: 주소, 화이트리스트는 룰, 메모, 등록·해제한 직원과 시각). 해제해도 지우지 않고 `active: false` |
| `eval_labels` | [EVAL] 봇이 보낸 정답 라벨 (지갑, 노린 룰, 시나리오) |

### 6-2 Redis

| 이름 | 내용 |
|---|---|
| `fds:w:send / split / out / in:{지갑}` | 1단계 5분 기록 (6분 뒤 자동 삭제) |
| `fds:hold:{지갑}` | 보류 중 (값 = 룰, 3분 뒤 자동 삭제) |
| `fds:frozen_wallets`, `fds:frozen_reasons` | 동결 지갑, 사유 (지갑 → L1~L8, MANUAL) |
| `fds:frozen_confirmed` | 직원이 확인한 동결 (`L5_EXPOSURE=confirmed`일 때 L5 근거) |
| `fds:unfrozen_at` | 직원이 동결 해제한 시각 (지갑 → ms). Spark는 이 시각 이후 기록만 분석 |
| `fds:blacklist_wallets` | 블랙리스트 |
| `fds:whitelist:R01` ~ `R06`, `fds:whitelist:L` | 1단계 룰별 면제, 2단계 분석 면제 |
| `fds:analyzed:{지갑}`, `fds:analyze_pending` | 재분석 간격, 미룬 지갑 (점수 = 분석할 시각) |
| `fds:recheck_l5`, `fds:blacklist_seen`, `fds:frozen_confirmed_seen` | 계기 ③ 대기 목록, 이미 처리한 블랙리스트·확인된 동결 |
| `fds:heartbeat:spark` | Spark 마지막 배치 시각 (ms). 시스템 상태 화면이 읽음 |
| `idemp:*` | 중복 확인 키, 1시간 뒤 삭제 |

### 6-3 Kafka

| 토픽 | 내용 |
|---|---|
| `transfer.events.v1` | 통과한 거래 |
| `transfer.rejected.v1` | 거절된 요청 (401 제외). ClickHouse 장기 보관용, Spark는 읽지 않음 |
| `fds.holds.v1` | 1단계 보류(HOLD), R07 차단(BLOCK, 받는 지갑이 블랙리스트일 때) |
| `fds.alerts.v1` | Spark L1~L8 경보 |
| `fds.freezes.v1` | 동결(`FREEZE`, 자동 `AUTO` / 직원 `MANUAL`)과 해제(`UNFREEZE`). Spark는 `FREEZE`만 계기 ③으로 씀 |

- 같은 지갑의 메시지는 지갑 주소를 key로 보낸다.

### 6-4 ClickHouse (장기 이력)

Kafka 토픽을 ClickHouse가 직접 구독해 쌓는다. 서비스 코드는 ClickHouse에 쓰지 않는다.

| 테이블 | 읽는 토픽 | 내용 |
|---|---|---|
| `transfers` | `transfer.events.v1`, `transfer.rejected.v1` | 통과·거절 거래 |
| `holds` | `fds.holds.v1` | 1단계 보류·R07 차단 |
| `alerts` | `fds.alerts.v1` | Spark 경보 |
| `freezes` | `fds.freezes.v1` | 동결·해제 |

- 토픽마다 Kafka 엔진 테이블(`*_kafka`) → 물질화 뷰(`*_mv`) → 본 테이블 순서로 들어간다 (`clickhouse/*.sql`, 컨테이너가 처음 뜰 때 한 번 실행).
- 본 테이블은 `ReplacingMergeTree`라 같은 메시지가 두 번 와도 나중에 하나로 합쳐진다.
- `kafka.xml`로 토픽을 처음부터(`earliest`) 읽는다.
- 관제 화면 시스템 상태(`GET /health`)가 최근 1시간(적재 지연 5분 제외) 통과 거래 수를 Mongo와 ClickHouse에서 세어 비교한다.

---

## 7. 설정

### 7-1 기간 설정 (`PERIOD_PROFILE`)

| 설정 | 장기 | 하루 | 경유 | 재분석 간격 |
|---|---|---|---|---|
| `prod` (운영) | 2주 | 24시간 | 1시간 | 60초 |
| `demo` (시연) | 20분 | 5분 | 1분 | 15초 |

- 내 PC는 `docker-compose.yml`, AWS는 k3s ConfigMap에서 바꾼다. 코드는 같다.
- 게이트웨이(관제 화면 표시)와 Spark(판정)에 **같은 값**을 넣는다. 지금은 compose·k3s 모두 `demo`다.

### 7-2 바꾸기 쉬운 값

| 값 | 현재 | 위치 |
|---|---|---|
| 시각 허용 범위 `MAX_CLOCK_SKEW` | 30초 | `app/main.py` |
| 1단계 기준, 5분, 보류 시간 | R01 5건, R02 3회, R03·R04 5개, 3분 | `app/holds.py` |
| R05·L2 공통 기준 (env `R05_LIMIT_USDT`) | 100,000 USDT | `docker-compose.yml`, `common/keys.py` |
| 보류 기록 보관 `HOLDS_KEEP_DAYS` | 14일 | `app/store.py` |
| `transfers` 자동 삭제 `TRANSFERS_TTL_DAYS` | 0 (끔). ClickHouse 연결 후 14 | `docker-compose.yml` (gateway) |
| 2단계 기준, 기간, 처리량 제한 | L1~L8 숫자, `PROFILES`, 배치당 지갑 2,000개 | `spark/config.py` |
| 자동 동결 룰 `AUTO_FREEZE_RULES` | L1, L2, L3, L8 | `app/alert_consumer.py` |
| 위험도 `RISK_LEVELS` (L5는 등급 사용) | L8 CRITICAL, L1·L2·L3 HIGH, L4~L7 MEDIUM | `app/wallet_status.py` |
| 사건 분리 기준 `CASE_GAP` | 10분 | `app/alert_consumer.py` |

### 7-3 실험·운영 스위치 (환경 변수)

| 변수 | 기본값 | 뜻 | 위치 |
|---|---|---|---|
| `L1_ACTION` | `freeze` | `review`면 L1을 자동 동결하지 않고 검토 대기로 | `app/alert_consumer.py`, compose(alert-consumer) |
| `L1_HOLD_SIDE` | `sender` | L1이 셀 보류: `sender`(보낸 쪽만) / `all` | `spark/config.py`, compose(spark) |
| `L5_EXPOSURE` | `all` | L5 동결 지갑 근거: `all`(자동 동결 포함) / `confirmed`(직원 확인 동결만, 비교 측정용) | `spark/config.py` |
| `CROSSCHECK_ENABLED` | `1` | `0`이면 Spark 재계산(`recompute`)을 끔 | `spark/config.py` |
| `EVAL_ENABLED` | `1` | `0`이면 [EVAL] 평가 주소가 생기지 않음 (운영은 0) | `app/eval_poc.py`, compose(gateway) |
| `HEALTH_*` | 신선도 60초/600초, outbox 30초/300초, 대조 60분(지연 5분) | 시스템 상태 경고·장애 기준 | `app/admin_views.py` |
| `STR_INSTITUTION_NAME` | `{고객사 거래소명}` | STR 초안의 보고 기관 이름 | `app/str_draft.py` |

---

## 8. 운영

명령은 저장소 맨 위 폴더에서 실행한다.

### 8-1 코드 반영

| 고친 곳 | 로컬 (compose) | k3s |
|---|---|---|
| `app/` | `docker compose up -d --build <서비스>` | 이미지 다시 빌드 후 넣기 |
| `spark/`, `common/` | `docker compose restart spark` | `docker compose build spark` → `deploy/k3s/import_images.sh` |
| `spark/requirements.txt` | `docker compose up -d --build spark` | 위와 같음 |

### 8-2 운영 명령

```bash
# 블랙리스트·화이트리스트 등록과 지갑 동결·해제는 직원 API로 한다.
# Redis에서 직접 빼면 사건 기록·해제 이벤트가 남지 않고, 해제 전 거래로 다시 동결될 수 있다.

# 보류 해제 (3분을 기다리지 않을 때)
docker exec aegis-redis redis-cli DEL fds:hold:<지갑주소>

# 경보 보기 (Ctrl+C로 종료)
docker exec aegis-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fds.alerts.v1 --from-beginning

# 로그. Spark 배치 로그의 seconds가 10초보다 충분히 짧은지, pending_size가 계속 늘지 않는지 본다.
docker logs aegis-alert-consumer --tail 30
docker logs aegis-spark --tail 30
```

### 8-3 Kafka 토픽 만들기

새 환경에서는 Kafka 토픽 자동 생성이 꺼져 있어 다섯 토픽을 직접 만든다. **Spark보다 먼저** 만들어야 한다 (없으면 Spark가 시작하다 멈춘다).

```bash
docker exec aegis-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic transfer.events.v1 --partitions 3 --replication-factor 1
docker exec aegis-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic transfer.rejected.v1 --partitions 3 --replication-factor 1
docker exec aegis-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic fds.holds.v1 --partitions 3 --replication-factor 1
docker exec aegis-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic fds.alerts.v1 --partitions 3 --replication-factor 1
docker exec aegis-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic fds.freezes.v1 --partitions 3 --replication-factor 1
```

### 8-4 주의

- 화이트리스트(거래소 등 룰별 면제)는 Redis에 있다. Redis를 비우거나 새 환경을 만들면 직원 API(`POST /whitelist`)로 다시 등록해야 한다.
- 관제 웹은 동결·해제를 Redis에 직접 쓰지 않고 직원 API를 부른다 (Redis·이벤트·사건을 한 번에 처리).
- Spark를 체크포인트 없이 처음 시작하면 그 이후 들어온 메시지만 읽는다 (`STARTING_OFFSETS=latest`).
- 게이트웨이는 `.env`에 `GATEWAY_API_KEY`가 없으면 시작하지 않는다. `ADMIN_API_KEY`가 없으면 직원 API만 꺼진다.
- `TRANSFERS_TTL_DAYS`를 0에서 14로 바꾸면 게이트웨이가 시작할 때 14일 지난 `transfers`를 지우기 시작한다. **ClickHouse에 거래·거절 기록이 쌓이는 걸 확인한 뒤에** 켠다. 다시 0으로 바꾸면 자동 삭제를 끈다.

---

## 9. 부록

### 11/23 통합 반영 (DE 브랜치 기능 합침)

| 항목 | 내용 | 위치 |
|---|---|---|
| 감사 원장 `policy_decisions` | 자동 동결(SYSTEM)·직원 조치(ANALYST)·블랙/화이트리스트 변경·STR 저장을 한 줄씩 쌓기만 함 (`decision_id` 고유) | `app/admin.py`, `app/alert_consumer.py`, `app/store.py` |
| 직원 조치 사유 필수 | 요청 본문 `rationale` (없으면 422). 관제 화면 버튼은 `note`를 넘기고, 비어 있으면 `관제 화면 조치 (사유 미입력)`으로 기록 | `app/admin.py`, `app/admin_views.py` |
| 감사 이력 화면 (`GET /audit`) | 사건 생성 + 감사 원장을 읽음 | `app/admin_views.py` |
| 근거 막대 기준값 | 사건에 저장된 `criteria`(Spark가 판정에 쓴 값)를 먼저 씀, 없으면 기존 상수 | `app/admin_views.py` |
| STR 초안 API | `GET`/`PATCH /api/v1/admin/cases/{id}/str-draft` — 사건 증거·기준값·계산 출처(엔진·설정·배치·시각) 그대로. 관제 화면 사건 상세에서 조회·다운로드·의견 저장 | `app/str_draft.py` |
| 실시간 경보 WebSocket | `WS /ws/admin` (헤더 `X-Admin-Key`), 새 사건·변경을 Mongo change stream으로 보냄. 관제 화면이 받아 열린 화면을 갱신 | `app/live_alerts.py` |
| [EVAL] 평가 | 봇 정답 라벨 → 룰별 성적표. `EVAL_ENABLED=0`이면 꺼짐 | `app/eval_poc.py` |
| 429 본문 `retry_after` | 남은 보류 초를 헤더와 본문에 같이 | `app/main.py` |
| 계산 출처 | Spark 경보에 `criteria`·`alerted_at`·`batch_id`, 사건에 `criteria`·`source` 저장 | `spark/rules.py`, `spark/detector.py`, `spark/stream.py`, `app/alert_consumer.py` |
| L8 위험도 | CRITICAL | `app/wallet_status.py` `RISK_LEVELS` |
| L6 | 평소 기록이 없는 새 지갑은 보지 않음 | `spark/rules.py` |
| **L5 근거 기본값** | `L5_EXPOSURE=all`(자동 동결 포함, 기존 방식). `confirmed`는 비교 측정용 | `spark/config.py` |
| **L1 보류 집계 기본값** | `L1_HOLD_SIDE=sender`(보낸 쪽 보류만) | `spark/config.py`, `docker-compose.yml` |
| 게이트웨이 healthcheck | compose가 `/healthz`를 10초마다 확인 | `docker-compose.yml` |
