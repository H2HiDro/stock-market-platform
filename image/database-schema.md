# Database Schema — Stock Market Platform

Hệ thống dùng **3 database** ở 3 vai trò khác nhau.

---

## 1. ScyllaDB — `stock_data` (Online Store / kho nóng)

> NoSQL, không có khóa ngoại. Các bảng liên kết logic qua `symbol`. Ghi chú PK/Clustering & CDC.

```mermaid
erDiagram
    stock_prices {
        text symbol PK "partition key"
        text timestamp PK "clustering DESC"
        double price
        text exchange
        int quote_type
        int market_hours
        double change_percent
        bigint day_volume
        double change
        bigint last_size
        text price_hint
        bigint producer_timestamp
        note cdc "enabled, TTL 120s"
    }
    stock_latest_prices {
        text symbol PK
        double price
        timestamp timestamp
        text exchange
        int quote_type
        int market_hours
        double change_percent
        bigint day_volume
        double change
        bigint last_size
        text price_hint
        bigint producer_timestamp
        note cdc "enabled, TTL 120s"
    }
    stock_daily_summary {
        text symbol PK "partition key"
        date trade_date PK "clustering DESC"
        double open
        double high
        double low
        double close
        bigint volume
        double change
        double change_percent
        double vwap
        text exchange
        int quote_type
        int market_hours
    }
    stock_prices_agg {
        text symbol PK "part of composite PK"
        date bucket_date PK "part of composite PK"
        text interval PK "1m/5m/1h/1d"
        timestamp ts PK "clustering ASC"
        double open
        double high
        double low
        double close
        bigint volume
        double vwap
    }
    stock_news {
        text stock_code PK "partition key"
        timestamp date PK "clustering DESC"
        text article_id PK "clustering DESC"
        text title
        text link
        boolean is_pdf
        text content
        float sentiment_score
        text pdf_link
        timestamp crawled_at
    }
```

---

## 2. PostgreSQL — `warehouse` (Offline Store / Star Schema)

> Có khóa ngoại thật. `dim_stock` là dimension, 3 bảng `fact_*` là fact.

```mermaid
erDiagram
    dim_stock ||--o{ fact_daily_prices : "stock_code"
    dim_stock ||--o{ fact_news : "stock_code"
    dim_stock ||--o{ fact_predictions : "stock_code"

    dim_stock {
        text stock_code PK
        text exchange
        int quote_type
        timestamp created_at
        timestamp updated_at
    }
    fact_daily_prices {
        text stock_code PK,FK
        date trade_date PK
        double open
        double high
        double low
        double close
        bigint volume
        double change
        double change_percent
        double vwap
        int market_hours
    }
    fact_news {
        text stock_code PK,FK
        date news_date PK
        text article_id PK
        text content
        float sentiment_score
    }
    fact_predictions {
        text stock_code PK,FK
        date prediction_date PK
        double predicted_price
        varchar model_version
        double confidence_score
        timestamp created_at
    }
```

---

## 3. SQLite — `stock_models.db` (Model registry, trong stock-prediction)

> Lưu lịch sử training & alpha. Liên kết logic qua mã cổ phiếu (nằm trong `model_name` / `stock_code`).

```mermaid
erDiagram
    train_stock_model {
        text model_name PK "vd StockModel_FPT_..._Transformer"
        text date_train "dd/mm/yyyy"
        real mse_loss
        text alphas "JSON list công thức"
    }
    stock_alpha_metrics {
        text stock_code PK
        text alpha_formula PK
        real rank_ic
        real pvalue
        int n_samples
        text date_calculated
    }
```

---

## Liên kết giữa 3 database (luồng dữ liệu)

```mermaid
flowchart LR
    subgraph Scylla["ScyllaDB (Online Store)"]
        SP[stock_prices]
        SL[stock_latest_prices]
        SD[stock_daily_summary]
        SA[stock_prices_agg]
        SN[stock_news]
    end
    subgraph PG["PostgreSQL (Offline Store)"]
        DIM[dim_stock]
        FDP[fact_daily_prices]
        FN[fact_news]
        FP[fact_predictions]
    end
    subgraph SQLite["SQLite (model registry)"]
        TSM[train_stock_model]
        SAM[stock_alpha_metrics]
    end

    SD -->|DAG ETL 23h| FDP
    SN -->|DAG ETL 23h| FN
    SD -.->|upsert| DIM
    FDP -->|training| TSM
    FN -->|sentiment feature| TSM
    TSM -->|predict| FP
    SAM -->|alpha tốt nhất| TSM
```
