# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** TRẦN TRUNG HIẾU  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```text
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 35.9s
  run 2/3 … 34.9s
  run 3/3 … 34.5s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` tăng row sau mỗi lượt chạy. Phiếu sự cố #1041: người trực bấm **Clear Task** trên Airflow rồi chạy lại → bảng phình to mãi, không có lỗi nào được báo. |
| **Nguyên nhân** | Model incremental **không khai báo `unique_key`** → dbt mặc định sinh câu **`INSERT` (append)**: chạy lại cùng một partition sẽ **ghi thêm** row mới thay vì **ghi đè** row cũ. Nguồn CDC có `op='u'` (1.310 bản ghi update): một ticket tạo ngày D1 rồi sửa ngày D2 lọt qua điều kiện lọc `_ingested_at` theo `run_date` **ở hai partition ngày khác nhau trong cùng một lượt chạy** → sinh 2 row cho 1 ticket. Chạy lại nhiều lượt → cộng dồn (append) mãi. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` (grain là **entity** — 1 row / 1 ticket — nên merge theo khoá tự nhiên `ticket_id`, bản ghi mới thay bản ghi cũ). `dags/ai_training_pipeline.py`: đặt `catchup=False` và `max_active_runs=1` (chỉ giảm tần suất kích hoạt lỗi, không phải root cause). |
| **Bằng chứng** | trước: 13.790 hàng / 1.310 ticket bị lặp · sau: **12.480 hàng**, 0 ticket lặp, 1 hàng/1 ticket ✓ · checksum 3 lượt giống hệt nhau: `8622572a97 8622572a97 8622572a97` |

Ghi chú cho thang 10đ mục Nguyên nhân: "Thêm `unique_key`" là *cách fix*, không phải root cause. Root cause là **phát biểu về cơ chế**: *incremental không có key → dbt sinh INSERT → chạy lại ghi thêm; kết hợp CDC `op='u'` làm cùng một ticket rơi vào 2 partition ngày trong một lượt → trùng lặp*. Hai tham số DAG chỉ hạn chế tần suất kích hoạt (backfill + ghi song song), không sửa được root cause.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` thiếu 455 hàng (8.645/9.100) so với đối chiếu thủ công; chỉ thiếu ở các **ngày cũ** (08-03 → 08-13), ngày mới thì đủ. Phiếu sự cố #1043. |
| **P99 độ trễ đo được** | **2.73 ngày** *(P50=0.13 ngày · P95=1.81 ngày · P999=2.92 ngày · max=2.94 ngày; 5.05% bản ghi tới kho muộn hơn 1 ngày; 0.0% muộn hơn 3 ngày)* |
| **Lookback đã chọn** | **3 ngày** — vì P99 = 2.73 ngày nên lùi 3 ngày bao phủ ~100% bản ghi; thực tế không có bản ghi nào muộn quá 3 ngày (max = 2.94 ngày). |
| **Nguyên nhân** | Điều kiện lọc incremental `event_date > (select max(event_date) từ bảng đích)` chỉ chấp nhận ngày **mới hơn** ngày lớn nhất đã có. Event xảy ra 08-12 nhưng tới kho 08-15: lúc chạy ngày 08-15, `max(event_date)` trong đích = 08-14 → 08-12 không ≥ 08-14 nên **không bao giờ được xử lý**; các lượt sau cũng bỏ qua mãi → 455 cặp (ngày, customer) mất hẳn vĩnh viễn. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: (1) nới điều kiện lọc thành `event_date >= (select max(event_date) ... ) - interval 3 day` để tính lại các ngày cũ có dữ liệu về muộn; (2) thêm `unique_key = ['event_date', 'customer_id']` + `incremental_strategy = 'merge'` — vì window rộng nên cùng cặp bị tính lại nhiều lần, merge giúp lần tính sau **thay thế** lần trước, không cộng dồn. |
| **Bằng chứng** | trước: **8.645 hàng** (thiếu 455) · sau: **9.100 hàng** ✓ · checksum 3 lượt giống hệt nhau `3db448685c` ✓ · `gold_training_set` vẫn 12.480 (nhiệm vụ 1 không vỡ) |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Chọn P99 (2.73 ngày) thay vì `max` (2.94 ngày) vì `max` là đại lượng **không chặn được** trong thực tế: nếu một bản ghi lạc tới sau 30 ngày thì lookback phải lùi 30 ngày và tốn công tính lại **ở mọi lượt chạy về sau** chỉ để phục vụ 1 outlier. P99 gói gọn 99% trường hợp thực tế với window nhỏ nhất. Với dữ liệu seed ở đây, P99=2.73 và max=2.94 đều nhỏ hơn 3, và tỷ lệ muộn >3 ngày = 0% nên lookback 3 ngày bao phủ toàn bộ mà vẫn tối thiểu. Chi phí của each ngày lookback: mỗi lượt chạy phải **tính lại** nhiều partition hơn (ở đây 4 ngày thay vì 1 ngày), gánh **mãi mãi ở mọi lượt về sau**; merge theo (event_date, customer_id) giữ bảng ổn định dù tính lại. |

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Cột `priority` từ 08-10 đổi từ số sang chuỗi; pipeline không dừng nhưng model phân loại dự đoán kém. `silver_tickets.priority` có NULL=6.488 và các giá trị 0/5/-1 trong khi contract quy định miền 1..4. Phiếu sự cố #1047. |
| **Nguyên nhân** | Backend đổi **cách biểu diễn** `priority` từ số (1..4) sang nhãn chuỗi (`urgent/high/medium/low`) từ 08-10 — schema evolution, ý nghĩa không đổi nhưng dữ liệu cũ không hề sai. Macro `normalize_priority` dùng `try_cast(priority_raw as integer)` nên sai theo hai hướng ngược nhau: (1) biến **nhãn chuỗi hợp lệ** thành NULL (6.488 hàng), và (2) **chấp nhận** `'0'/'5'/'-1'` vì chúng đúng là số — trong khi contract chỉ cho 1..4. Ngoài ra cả Silver lẫn Gold đều không có ràng buộc nào bắt giữ lỗi này (contract `enforced: false`, chưa có test). |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **Nhóm 1** — `'1' '2' '3' '4'` (6.846 bản ghi): đúng contract cũ → **giữ nguyên**. **Nhóm 2** — `'urgent' 'high' 'medium' 'low'` (7.142 bản ghi): **schema evolution**, ý nghĩa không đổi → **map** urgent=1, high=2, medium=3, low=4. **Nhóm 3** — `'0' '' 'P1' 'unknown' 'P2' '5' NULL '-1'` (312 bản ghi): **dữ liệu hỏng thật** → trả NULL → đưa vào `quarantine_tickets`. |
| **Cách khắc phục** | (1) `dbt/macros/normalize_priority.sql`: thay `try_cast` bằng khối `CASE` xử lý đủ 3 nhóm (macro dùng chung cho cả Silver lẫn quarantine nên không thể lệch nhau); (2) `dbt/models/silver/silver_tickets.sql`: thêm `where normalize_priority(...) is not null` **trước** `row_number()` — loại bản ghi hỏng chứ không loại cả ticket; (3) `dbt/models/silver/quarantine_tickets.sql`: thay `where false` bằng `where normalize_priority(...) is null`; (4) `dbt/models/silver/schema.yml`: `contract.enforced: true` (ràng buộc kiểu integer) + thêm test `not_null` và `accepted_values: [1,2,3,4]` (ràng buộc miền giá trị — contract một mình vẫn cho `priority=99` đi qua). |
| **Bằng chứng** | `quarantine_tickets` = **312** hàng (đúng expected, 1 hàng/1 bản ghi CDC) · `silver_tickets.priority`: NULL=6.488/0/5/-1 → hết · `dbt test` **11/11 pass** (tăng từ 9 → 11) · `silver_tickets` vẫn đủ **12.480** ticket · `gold_training_set` vẫn **12.480**, checksum 3 lượt giống nhau. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> **Bronze hay Silver?** Nên chặn/phân loại ở tầng **Silver** chứ không từ chối ở Bronze. Bronze có nhiệm vụ là **bảo toàn dữ liệu thô** — nếu Bronze vứt bỏ row lỗi thì sau này điều tra sự cố sẽ không còn dấu vết gì (mất dữ liệu gốc, mất cả dấu hiệu của vụ lỗi). Giữ nguyên Bronze, để Silver thực thi chuẩn hoá và đẩy row hỏng sang `quarantine_tickets` — vừa giữ được khả năng truy vết, vừa cô lập bản ghi xấu.
> **Sao không để `dbt test` fail và dừng DAG?** Về quy mô: 312 bản ghi hỏng không có quyền chặn 130.000+ event và 31.200 chunk hoàn toàn bình thường đến tay người dùng. Dừng pipeline nghĩa là mọi thứ (kể cả dữ liệu tốt) bị treo vì một lỗi nhỏ; thay vào đó, contract + test chạy như **báo động** (alert), còn `quarantine_tickets` là **hàng đợi** để người trực xử lý bản ghi xấu mà không làm gián đoạn luồng dữ liệu tốt. |

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

**Bài đã làm: A + B (cả hai)**

### Bài A — Query dashboard chậm

| | |
|---|---|
| **Triệu chứng** | Dashboard mất ~38s (baseline đo: `rows scanned` = 5.000.000, `files` = 5.000, `rows on disk` = 130.683). Phiếu sự cố #1052. |
| **Nguyên nhân** | (1) `data/gold_events/` là **5.000 file Parquet nhỏ** (~26 hàng/file) không partition, tên file `part-0000X.parquet` không mang thông tin lọc → engine buộc mở **toàn bộ** file rồi mới biết file nào có ích (small-file problem: DuckDB đọc tròn lên theo lô nên 5.000 file tí hon tốn 5.000.000 đơn vị công quét cho dataset 130.683 hàng); (2) điều kiện lọc `strftime(event_time,'%Y-%m-%d') = '2026-08-09'` **bọc cột trong hàm** → không sargable: engine không so được với min/max statistics của row group. |
| **Cách khắc phục** | `tools/compact.py`: `COPY ... PARTITION_BY (event_date)` — event_date chỉ có 14 giá trị → **14 thư mục** (không partition theo `customer_name` vì 650 giá trị → tái tạo small-file); `ORDER BY customer_name, event_time` để các hàng cùng khách liền nhau → min/max row group "khít" trên `customer_name`; `ROW_GROUP_SIZE 2048` (1 ngày ~9.300 hàng — nếu để mặc định 122.880 thì cả ngày là 1 row group, mất tác dụng lọc). Compact chuyển đổi **tại chỗ** `data/gold_events` (đọc cả trạng thái phẳng lẫn partition nên chạy lại được, xoá file phẳng cũ) — nhờ đó `queries/dashboard.sql` chỉ trỏ một chỗ `data/gold_events/**/*.parquet` + `hive_partitioning = true`, viết lại predicate **sargable**: `event_date = date '2026-08-09'` (lọc thẳng cột partition → engine prune theo đường dẫn, chỉ mở 1/14 thư mục). Thiết kế này bảo đảm `make verify` chạy được cả trước lẫn sau `make compact` (không lỗi thiếu file). |
| **Bằng chứng** | `rows scanned`: **5.000.000 → 9.324** (giảm **536×**, cần ≥10×) · `files`: **5.000 → 14** · `rows on disk`: 130.683 không đổi · **`result hash`**: `4379e4c5d9f3` → `4379e4c5d9f3` (KHÔNG đổi — ngữ nghĩa giữ nguyên) · thời gian ~4.588ms → 25ms. |

### Bài B — Consumer bị giết giữa batch

| | |
|---|---|
| **Triệu chứng** | `make crash-test` ban đầu: **mất 500 hàng** (C = 19.500 ≠ A = 20.000), không trùng → CHƯA ĐẠT. |
| **Nguyên nhân** | Thứ tự trong `consume()`: `commit() → maybe_crash() → write_batch()`. **At-most-once**: offset được commit **trước** khi dữ liệu được ghi; tiến trình chết tại `maybe_crash()` sau khi đã commit lô 7 nhưng chưa ghi lô 7 → lần khởi động lại đọc từ offset 3.500 và **không bao giờ ghi lại lô 7** → mất 500 bản ghi vĩnh viễn. |
| **Cách khắc phục** | (a) Đảo thứ tự trong `consume()` thành `write_batch() → maybe_crash() → commit()`: ghi trước, commit sau → **at-least-once** (crash giữa ghi và commit khiến lần restart đọc lại lô đó). (b) `write_batch()` thành phép ghi **idempotent**: `DELETE ... WHERE event_id IN (...)` rồi `INSERT` lại cả lô → phát lại một lô sẽ xoá đúng dòng cũ rồi ghi bản mới, không trùng không mất, "nội dung mới thắng" (ngữ nghĩa tương đương `ON CONFLICT DO UPDATE`). Không dùng `ON CONFLICT`/primary key vì **đo được** DuckDB xử lý uniqueness từng dòng rất chậm (~6s/lô 500 hàng, executemany thêm ~5×) — delete+insert chạy theo lô nhanh hơn nhiều. |
| **Bằng chứng** | Trước: mất 500 hàng, `C ≠ A` → **CHƯA ĐẠT** · Sau: A=20.000, C=**20.000**/20.000 event_id, không mất ✓ không trùng ✓ `C == A` ✓ → **ĐẠT ✓** · `make verify` vẫn 4/4 tiêu chí (3 nhiệm vụ chính không bị ảnh hưởng). |

**Trả lời câu hỏi DO UPDATE vs DO NOTHING:** khi một message bị **replay với nội dung đã đổi** — `DO UPDATE` áp dụng nội dung mới nhất (bản ghi cuối thắng, hội tụ về trạng thái mới); `DO NOTHING` giữ nguyên giá trị ghi lần đầu → **dữ liệu cũ tồn đọng**. Với nguồn message có thể thay đổi nội dung (ví dụ latency_ms được sửa lại khi retry), phải chọn ngữ nghĩa "nội dung mới thắng" (tương đương DO UPDATE) — đây chính là lý do phép ghi delete+insert trong `write_batch` xoá rồi ghi lại thay vì bỏ qua lô đã có.

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Đọc `config()` của từng model gold: có `unique_key` + `incremental_strategy` hay không, grain là gì — vì model incremental không khai key sẽ ghi thêm (append) thay vì ghi đè. |
| 2 | Đo **phân bố độ trễ** (event_time → _ingested_at) và đọc điều kiện lọc trong `is_incremental()`: nếu filter chỉ lấy ngày > max hiện có thì dữ liệu về muộn (late-arriving) sẽ bị bỏ vĩnh viễn; cần lookback theo P99 + merge theo composite key. |
| 3 | Kiểm tra **dữ liệu thô** của cột mà contract ràng buộc (ở Bronze) trước khi tin vào kết quả: `group by priority_raw` xem có nhiều cách biểu diễn cùng một ý nghĩa không (schema evolution), phân biệt "đổi định dạng" vs "dữ liệu hỏng" — rồi mới đặt chuẩn hoá + contract + quarantine. |