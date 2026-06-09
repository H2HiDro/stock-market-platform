# Tổng Quan Hệ Thống — Real-time Stock Prediction Platform

> Tài liệu lý thuyết tổng hợp, tổ chức theo **3 pipeline**: **Data → Training → Web**.
> Nguồn: phân tích trực tiếp source code repo + bài viết tối ưu của tác giả + sơ đồ `image/pipeline.svg`.

---

## 0. Tổng quan

Hệ thống thu thập, xử lý và dự đoán giá cổ phiếu **285 mã sàn HOSE** theo thời gian thực, tích hợp quy trình **MLOps**. Tác giả chia hệ thống thành **3 pipeline theo chức năng**:

```
┌─ DATA PIPELINE ────────── thu thập + xử lý + lưu trữ
│   yfinance/news → Kafka → Flink → ScyllaDB → (ETL) → PostgreSQL
├─ TRAINING PIPELINE ────── huấn luyện + dự đoán (ML)
│   PostgreSQL → Gemini sinh alpha → train LSTM/Transformer → fact_predictions
└─ WEB PIPELINE ─────────── phục vụ giao diện
    ScyllaDB ─CDC→ Backend (FastAPI) ─WS/REST→ Frontend (Next.js)
```

| Khái niệm | Thực thể | Vai trò |
|---|---|---|
| **Online Store** | ScyllaDB | Kho nóng, realtime serving |
| **Offline Store** | PostgreSQL warehouse | Kho lạnh, lịch sử + training |
| **Model registry** | SQLite + Google Drive | Lưu metadata + file model |

**Tech stack:** Python 3.11, Java 11, Rust, TypeScript · Kafka (KRaft) · Flink 1.17 · ScyllaDB 5.2 · PostgreSQL 14 · Airflow 2.10 · PyTorch · Google Gemini · Next.js 14 · Docker Compose · Cloudflare Tunnel.

---

## 1. DATA PIPELINE — Thu thập, xử lý & lưu trữ

Có **2 nhánh đầu vào song song** rồi hội tụ tại kho dữ liệu.

### 1.1 Nhánh giá (real-time)

| Bước | Service | Mô tả | Code |
|---|---|---|---|
| 1. Producer | `stock-websocket-service` | 3 producer kết nối **Yahoo Finance WebSocket**, mỗi tick → đóng gói **Avro 12 trường** + gắn `producer_timestamp` (đo latency), đẩy vào topic `yfinance` | [stock_producer.py](../stock-websocket-service/stock_producer.py) |
| 2. Hàng đợi | `kafka-service` | Kafka **KRaft** (3 controller + 3 broker, không Zookeeper) + **Schema Registry** | [docker-compose.yml](../kafka-service/docker-compose.yml) |
| 3. Xử lý | `flink-service` | Flink (Java, parallelism 12) lọc rác → 4 sink ghi ScyllaDB | [StockProcessingJob.java](../flink-service/src/main/java/com/stock/StockProcessingJob.java) |
| 4. Lưu | `scylla-service` | ScyllaDB cluster 3 node, keyspace `stock_data`, CDC trên 2 bảng | [scylla_setup.py](../scylla-service/scylla_setup.py) |

**Cơ chế Avro + Schema Registry (điểm cốt lõi):**
- Producer đăng ký schema 1 lần → nhận **Schema ID**.
- Mỗi message Kafka chỉ chứa `Schema ID + Data` (không kèm schema) → nhẹ.
- Flink nhận message → **Pull schema** từ Registry theo ID để giải mã.
- → Schema đi 1 đường, dữ liệu đi 1 đường; 2 bên luôn nói chung "ngôn ngữ".

**Flink "Transform Data" làm gì:**
- Lọc rác: `price > 0 AND day_volume > 0 AND price < 1000000 AND ABS(change_percent) < 50`.
- `keyBy(symbol)` rồi ghi **4 sink** (async, fire-and-forget):

| Sink | Bảng ScyllaDB | Nội dung |
|---|---|---|
| StockPricesSink | `stock_prices` | mọi tick thô |
| StockLatestSink | `stock_latest_prices` | giá mới nhất mỗi mã (`reduce`) |
| StatefulDailyAggregator | `stock_daily_summary` | OHLCV ngày (`ValueState`) |
| StatefulMinuteAggregator | `stock_prices_agg` | nến 1 phút |

### 1.2 Nhánh tin tức (scraper)

| Bước | Mô tả | Code |
|---|---|---|
| Crawl | DAG chạy **mỗi giờ**, crawl **Vietstock**, 285 mã, 10 luồng; phân biệt bài text / PDF | [newscrawler.py](../task-daily-service/dags/news_stock/newscrawler.py) |
| Phân tích | **Gemini** chấm sentiment (−1..1) cho text & PDF báo cáo tài chính | [DAGs_newstock.py](../task-daily-service/dags/DAGs_newstock.py) |
| Lưu | Ghi bảng `stock_news` (ScyllaDB) | |

### 1.3 Điểm hội tụ — ETL sang Offline Store

- DAG `DAGs_warehouse` chạy **23h hằng ngày** (⚠️ sơ đồ ghi "15h"), **incremental**:
  - `stock_daily_summary` → `fact_daily_prices` + upsert `dim_stock`
  - `stock_news` → `fact_news`
- Chỉ đẩy **dữ liệu tổng kết ngày + tin tức**, KHÔNG đẩy tick thô.
- → [DAGs_warehouse.py](../task-daily-service/dags/DAGs_warehouse.py)

### 1.4 Câu chuyện tối ưu latency: 17s → 195ms

Tác giả đo bằng `producer_timestamp` (t1) ↔ CDC output (t3):
1. **Bỏ ScyllaDB Kafka Sink Connector → viết CDC printer bằng Rust** (3 lựa chọn: Kafka Connect / Go / Rust). DB→output còn **30ms**.
2. **Bỏ PyFlink → viết lại toàn bộ bằng Native Java** (PyFlink chậm do IPC serialize Java↔Python mỗi event).
3. Kết quả: **17s → ~195ms** (README ghi "< 100ms" — hơi vênh).

Bài học: dùng **ngôn ngữ gốc** của công cụ (Java cho Flink, Rust cho tác vụ tốc độ cao); **đo mọi thứ**.

---

## 2. TRAINING PIPELINE — Huấn luyện & dự đoán (ML)

**Kích hoạt:** DAG `DAGs_prediction` chạy **23h**, kiểm tra mai có phải ngày giao dịch (`holidays.VN`) → gọi `POST /stock-prediction`. 285 mã chia **5 batch**, mỗi batch 1 process (GPU). → [api.py](../stock-prediction/api.py)

### 2.1 Sinh & chọn Alpha (LLM + RankIC)

| Bước | Mô tả | Code |
|---|---|---|
| Collect Data | Đọc warehouse (`fact_daily_prices` + `fact_news`), tính chỉ báo kỹ thuật (SMA, EMA, RSI, MACD, Bollinger, OBV), thêm sentiment 5 công ty liên quan | `collect_data()` [utils.py](../stock-prediction/model/utils.py) |
| Sinh Alpha | **Gemini** sinh 5 công thức alpha (biểu thức số học, vd `(close - SMA_20)/SMA_20`) | `generate_stock_alphas()` |
| Chấm điểm | Tính **RankIC** (Spearman corr giữa alpha và lợi suất tương lai) cho từng công thức | `calculate_ic_from_column()` |
| Chọn top 5 | Gộp alpha mới + alpha cũ (SQLite) + sinh thêm → sort RankIC → lưu top 5 vào `stock_alpha_metrics` | `select_best_alphas()` |

### 2.2 Huấn luyện 2 model

- 5 alpha tốt nhất = feature. Dựng dataset (`window_size=5`).
- Train **song song Transformer + LSTM** (2 thread), 30 epoch, `MSELoss`, Adam lr=1e-4.
- → [auto_pipeline.py](../stock-prediction/model/auto_pipeline.py), [models.py](../stock-prediction/model/models.py)

| Model | Kiến trúc |
|---|---|
| **StockTransformer** | Encoder-Decoder; ConvEmbedding + PositionalEncoding + TemporalEmbedding (ngày/tuần/tháng); d_model=512, 8 head, 3 layer |
| **StockLSTM** | Encoder-Decoder LSTM, hidden=512, 3 layer + TemporalEmbedding |

### 2.3 Chọn model & dự đoán

1. Chọn model có **val loss thấp hơn** trong 2 cái.
2. So với **model hôm qua** (metadata ở SQLite `train_stock_model`, file ở Google Drive):
   - model mới tốt hơn → đẩy checkpoint mới lên **GG Drive** (rclone), dùng dự đoán.
   - model cũ tốt hơn → **tải model cũ về** dùng lại.
3. Dự đoán giá đóng cửa phiên kế tiếp → ghi `fact_predictions` (`confidence = 1/(1+loss)`).

### 2.4 Vai trò lưu trữ
- **SQLite** (`stock_models.db`): sổ ghi chép — alpha (`stock_alpha_metrics`) + lịch sử model (`train_stock_model`).
- **Google Drive**: kho file checkpoint `.pt`.

---

## 3. WEB PIPELINE — Phục vụ giao diện

```
ScyllaDB ─CDC→ Rust printer → Backend (FastAPI :8005) ─WS/REST→ Frontend (Next.js :3005)
```
Expose ra ngoài qua **Cloudflare Tunnel**. → [web-stockAI](../web-stockAI/)

### 3.1 Backend (FastAPI :8005)

| Nhóm | Endpoint chính | Nguồn |
|---|---|---|
| Giá (REST) | `/stocks/get_reference`, `/stocks_latest`, `/stocks_VN30`, `/stock_price_by_symbol`, `/stock_daily_by_symbol`... | ScyllaDB |
| Dự đoán | `/stock_predictions`, `/stock_predictions_history`, `/stock_predictions_accuracy` | PostgreSQL |
| Thông tin | `/stock_info/{symbol}` (~60 chỉ số) | Yahoo Finance |
| Tin tức | `/news/sectors`, `/news_new`, `/news_time_filtered` | ScyllaDB |
| Real-time | `/stocks/ws/stocks_realtime` (WebSocket) | CDC printer |
| Metrics | `/stocks/metrics` (Prometheus, `cdc_latency_ms`) | |

**WebSocket real-time:**
- Chỉ chấp nhận kết nối **trong giờ giao dịch VN 9h–15h, T2–T6**; ngoài giờ trả lỗi rồi đóng.
- Client đầu tiên → backend **spawn subprocess Rust CDC printer** (đọc `stock_latest_prices`), parse output, **broadcast** JSON tới mọi client; đo `cdc_latency_ms`.
- → [stocks.py](../web-stockAI/backend/src/api/routers/stocks.py)
- ⚠️ Backend **chưa có authentication** (`auth.py` rỗng, CORS `*`).

### 3.2 Frontend (Next.js 14 + Tailwind 4 + shadcn/ui :3005)

| Trang | Nội dung |
|---|---|
| `/` | Hero + search + 5 widget (top 5 **giá cao nhất**) |
| `/stocks` | Bảng giá toàn sàn; lọc **ALL/VN30** + **ngành**; cột PREDICT; flash xanh/đỏ |
| `/stock/[symbol]` | 7 tab: Trading (chart realtime + lệnh khớp), History (OHLCV, chart/table), Statistics, Financial, Company, Predict, News |
| `/analysis` | Market Summary, Price Change, Top Volume, Sector Distribution/Volume/Trend |
| `/dashboard` | Tổng quan, watchlist, alerts |
| `/news` | Tin tức + sentiment, lọc ngành/thời gian |

**Hook chính:** `useStocksRealtimeWS` (bảng giá + WS), `useStockData` (chi tiết mã), `usePredictions` / `useAccuracy` (ML), `useStockChartData` (sparkline).
**Quy ước giá:** trần = TC×1.07, sàn = TC×0.93 (biên ±7% HOSE), tính ở frontend.

---

## 4. Ba Database

### ScyllaDB `stock_data` (Online Store — NoSQL, không FK)
```
stock_prices        PK(symbol, timestamp↓)               [CDC 120s]
stock_latest_prices PK(symbol)                            [CDC 120s]
stock_daily_summary PK(symbol, trade_date↓)
stock_prices_agg    PK((symbol,bucket_date,interval), ts)
stock_news          PK(stock_code, date↓, article_id↓)
```
> Dữ liệu base **không TTL** (lưu lâu dài); chỉ CDC log có TTL 120s.

### PostgreSQL `warehouse` (Offline Store — Star Schema, có FK)
```
dim_stock (stock_code PK)
 ├──< fact_daily_prices (stock_code+trade_date)
 ├──< fact_news         (stock_code+news_date+article_id)
 └──< fact_predictions  (stock_code+prediction_date)
```

### SQLite `stock_models.db` (Model registry)
```
train_stock_model   (model_name PK, mse_loss, alphas JSON, date_train)
stock_alpha_metrics (stock_code+alpha_formula PK, rank_ic, pvalue, n_samples)
```

> Sơ đồ ER chi tiết: [database-schema.md](../image/database-schema.md)

---

## 5. Các vấn đề & điểm yếu phát hiện

### 🔴 Nghiêm trọng — chất lượng model
- **Data leakage**: `random_split` chia train/val **ngẫu nhiên** trên chuỗi thời gian → model "nhìn trộm tương lai". Hệ quả thực tế: **accuracy chỉ 47.7%** (thấp hơn đoán ngẫu nhiên 50%).
- **Model thiên lệch**: ~87% mã được dự đoán "tăng" (248 up / 37 down), trong khi thị trường thực tế đa số giảm.
- **Dữ liệu quá ngắn**: warehouse mới có dữ liệu từ ~10/2025; mỗi mã 1 model riêng → thiếu dữ liệu.
- **So sánh không đồng nhất**: model mới đo *val loss*, model cũ lưu *mse_loss*.
- `confidence = 1/(1+loss)` là công thức tùy tiện, không phải xác suất.

### 🟠 Chất lượng dữ liệu
- Bộ lọc Flink `ABS(change_percent) < 50` quá lỏng → lọt giá trị **−21%** (vượt biên ±7% HOSE).
- Exchange hardcode `"NASDAQ"` cho cổ phiếu VN (Flink daily sink + DAG news).
- Phân ngành theo Yahoo (GICS), không theo chuẩn ICB Việt Nam; mapping **lệch giữa frontend và backend** (vd MSN).

### 🟡 Code / kiến trúc
- Mảng VN30 / 285 mã / mapping ngành **hardcode ~7 bản** rải khắp code → khó đồng bộ.
- Flink ghi ScyllaDB **fire-and-forget** (không chờ, không retry) → mất dữ liệu âm thầm; dùng `RichMapFunction` thay vì Sink chuẩn → không exactly-once.
- Producer **không gắn key** khi gửi Kafka → không đảm bảo thứ tự tick theo mã.
- Race condition: ETL & training cùng đặt 23h, không ràng buộc thứ tự.
- Nhiều đường dẫn hardcode `/home/obito/...`.

### 🔵 Đã sửa khi dựng demo
- **Thiếu folder `lib/`** frontend (utils/api/config) → đã tạo lại.
- `dashboard/page.tsx` thiếu `"use client"` → đã thêm.
- Header check giờ mở cửa 9:30 lệch hệ thống 9:00 → đã sửa về 9:00.
- `useStocksRealtimeWS` chỉ kết nối WS lúc mount → đã thêm interval tự kết nối khi vào phiên.

---

## 6. Ghi chú vận hành

- **Yêu cầu:** RAM ≥ 16GB, disk ≥ 50GB, GPU NVIDIA (cho training), Docker Compose, network `financi-network`.
- **Real-time chỉ chạy** trong giờ giao dịch VN (9h–15h, T2–T6) — ngoài giờ WS tự đóng.
- **API key cần:** Google Gemini (alpha + news sentiment), Cloudflare token, (tùy chọn) W&B.
- **Cổng:** Frontend 3005 · Backend 8005 · Flink UI 8088 · Kafka UI 8080 · Airflow 8087 · Grafana 1020 · Prediction API 8006.
- **Lưu ý version:** website deploy (`stock.kytran.io.vn`) **mới hơn repo** — một số widget/biểu đồ trên web (Total Stocks, Today's Prediction, Accuracy vs Reality, đổi LineChart→BarChart) chưa có/khác trong code repo.

---

*Tài liệu sinh từ phân tích source code — phản ánh đúng repo tại thời điểm phân tích. Một số chi tiết giao diện có thể khác bản web đang chạy.*
