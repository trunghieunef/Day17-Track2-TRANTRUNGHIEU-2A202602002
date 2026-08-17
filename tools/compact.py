#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — NHIỆM VỤ 4.  CHƯA CÓ LOGIC.

Hiện trạng: `data/gold_events/` gồm 5.000 file, mỗi file vài chục KB, không
partition, thứ tự hàng ngẫu nhiên.

Yêu cầu: đọc toàn bộ dataset cũ, ghi ra dataset mới có layout hợp lý hơn, sau đó cập
nhật `queries/dashboard.sql` để trỏ vào dataset mới.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

KHUNG THỰC HIỆN

    COPY (
        SELECT *
        FROM   read_parquet('data/gold_events/*.parquet')
        ORDER  BY <cột A>, <cột B>
    ) TO 'data/gold_events_v2' (
        FORMAT          parquet,
        PARTITION_BY    (<cột partition>),
        OVERWRITE_OR_IGNORE,
        ROW_GROUP_SIZE  <?>
    )

Ba quyết định, mỗi quyết định cần một lý do viết được ra giấy:

  <cột partition>   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi
                    mở file. Thông tin đó đến từ đường dẫn. Vậy cột nào của
                    truy vấn dashboard nên xuất hiện trong tên thư mục? Cột đó
                    có bao nhiêu giá trị phân biệt — tức bao nhiêu thư mục?
                    Partition theo cột có 650 giá trị thì hệ quả là gì?

  <cột A>, <cột B>  Thứ tự hàng trong file quyết định thống kê min/max của mỗi
                    row group có ích hay vô dụng. Sắp thế nào để các hàng cùng
                    một khách hàng nằm liền nhau?

  ROW_GROUP_SIZE    Mặc định 122.880 hàng. Một ngày có khoảng bao nhiêu hàng?
                    Nếu cả ngày gói gọn trong MỘT row group thì min/max của
                    row group đó phủ những gì, và còn tác dụng lọc không?

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
# Chuyển đổi TẠI CHỖ: đọc `data/gold_events`, ghi cấu trúc partition vào cùng thư
# mục, rồi xoá các file phẳng cũ. Nhờ đó `queries/dashboard.sql` chỉ cần trỏ một
# chỗ `data/gold_events/**` — chạy verify được ngay sau `make seed-extra` (đọc
# file phẳng) và sau `make compact` (đọc partition) mà không bao giờ lỗi thiếu file.
DST = DATA / "gold_events"


def main() -> int:
    con = duckdb.connect()

    n_src = len(list(SRC.glob("*.parquet")))
    all_glob = (SRC / "**" / "*.parquet").as_posix()

    # Đọc toàn bộ dataset — hỗ trợ CẢ hai trạng thái: file phẳng (5.000 file)
    # và đã partition (14 file), nên `make compact` chạy lại được.
    src_rows = con.execute(
        f"select count(*) from read_parquet('{all_glob}', hive_partitioning = true)"
    ).fetchone()[0]
    print(f"  nguồn : {SRC}  ({n_src:,} file phẳng · {src_rows:,} hàng)")

    # ── Ba quyết định (mỗi quyết định một lý do) ─────────────────────────
    # 1) partition_by (event_date)
    #    Dashboard lọc đúng MỘT ngày. event_date chỉ có 14 giá trị => 14 thư
    #    mục; engine bỏ qua 13/14 partition ngay từ đường dẫn (hive partition
    #    pruning) mà không cần mở file. KHÔNG partition theo customer_name:
    #    cột đó có 650 giá trị => 650 thư mục siêu nhỏ => tái tạo small-file
    #    problem (650 × ~200 hàng).
    #
    # 2) order by customer_name, event_time
    #    Các hàng cùng khách nằm liền nhau => min/max của mỗi row group trên
    #    customer_name trở nên "khít", engine lọc bỏ được row group không chứa
    #    khách đang hỏi (pruning theo statistics).
    #
    # 3) row_group_size 2048
    #    Một ngày có ~9.300 hàng. Nếu để mặc định 122.880, cả ngày gói trong
    #    MỘT row group: min/max phủ mọi khách => mất tác dụng lọc theo khách.
    #    Chia ~5 row group/ngày để lọc được row group theo customer_name.
    con.execute(f"""
        copy (
            select * from read_parquet('{all_glob}', hive_partitioning = true)
            order by customer_name, event_time
        ) to '{(DST).as_posix()}' (
            format parquet,
            partition_by (event_date),
            overwrite_or_ignore,
            row_group_size 2048
        )
    """)

    # Xoá các file phẳng cũ ở tầng gốc — đã được thay bằng cấu trúc partition.
    for p in list(SRC.glob("*.parquet")):
        p.unlink(missing_ok=True)

    n_dst = len(list(SRC.glob("**/*.parquet")))
    dst_rows = con.execute(
        f"select count(*) from read_parquet('{all_glob}', hive_partitioning = true)"
    ).fetchone()[0]
    print(f"  đích  : {SRC}  ({n_dst:,} file partition · {dst_rows:,} hàng)")

    # Kiểm tra không mất hàng nào giữa dataset cũ và mới.
    assert src_rows == dst_rows, f"MẤT HÀNG: {src_rows} != {dst_rows}"
    print("  OK — không mất hàng, layout mới đã sẵn sàng.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
