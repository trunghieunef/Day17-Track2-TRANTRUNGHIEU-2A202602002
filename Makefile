# ============================================================================
# LAB 17 — Data Pipeline Engineering
# Makefile tương thích CẢ Windows (cmd/Git Bash) lẫn Linux/macOS.
#
# Trên Windows:  python  + venv .venv/Scripts/
# Trên Linux:    python3 + venv .venv/bin/
# Không cưỡng ép SHELL (để make dùng shell mặc định: cmd trên Windows thuần,
# sh trên Linux, sh của msys trong Git Bash). Không phụ thuộc rm/grep/awk.
# ============================================================================

VENV    := .venv

ifeq ($(OS),Windows_NT)
    SYSPY  := python
    VENVPY := $(VENV)/Scripts/python.exe
else
    SYSPY  := python3
    VENVPY := $(VENV)/bin/python
endif

# Dùng `$(VENVPY) -m pip` / `-m dbt` thay cho .exe để đường dẫn không lệ thuộc OS.
PY  := $(VENVPY)
PIP := $(VENVPY) -m pip
DBT := $(VENVPY) -m dbt

# Forward-slash để cả cmd lẫn sh đều nhận được path hợp lệ.
export LAB17_DB := $(CURDIR)/warehouse.duckdb
export DBT_PROFILES_DIR := $(CURDIR)/dbt

# Bắt mọi tiến trình Python (kể cả dbt chạy trong-process) dùng UTF-8 làm
# encoding mặc định. Trên Linux đây là mặc định sẵn; trên Windows mặc định là
# cp1252 nên dbt đọc file có comment tiếng Việt (UTF-8) sẽ văng
# 'UnicodeDecodeError: charmap' ở giai đoạn parse manifest.
export PYTHONUTF8 := 1

.DEFAULT_GOAL := help
.PHONY: help setup seed seed-extra pipeline verify quick explain plan dbt-test \
        dbt-docs crash-test compact reset clean

help:  ## danh sách lệnh
	@echo ""
	@echo "  LAB 17 - Data Pipeline Engineering"
	@echo ""
	@echo "    make setup       venv + thu vien + sinh du lieu (chay 1 lan)"
	@echo "    make pipeline    chay duong ong mot luot"
	@echo "    make verify      xoa kho, chay 3 luot, in bang cham diem"
	@echo "    make quick       nhu verify nhung chi 1 luot"
	@echo "    make seed        sinh lai du lieu seed"
	@echo "    make seed-extra  [mo rong] sinh them du lieu ~30 giay"
	@echo "    make explain     [mo rong] do rows scanned cua dashboard.sql"
	@echo "    make plan        [mo rong] explain + cay EXPLAIN ANALYZE"
	@echo "    make compact     [mo rong] chay tools/compact.py"
	@echo "    make dbt-test    chay dbt test"
	@echo "    make dbt-docs    dung va mo tai lieu dbt"
	@echo "    make crash-test  [mo rong] kich ban consumer bi giet giua batch"
	@echo "    make reset       xoa kho DuckDB (giu lai seed va data/)"
	@echo "    make clean       xoa kho + target dbt + thu muc lam viec"
	@echo ""

setup:  ## venv + thư viện + sinh dữ liệu (chạy một lần)
	@$(SYSPY) -m venv $(VENV)
	@$(PIP) install -q --upgrade pip
	@$(PIP) install -q -r requirements.txt
	@$(PY) seed/generate.py
	@echo ""
	@echo "  xong. Bước tiếp theo:  make pipeline  rồi  make verify"

seed:  ## sinh lại dữ liệu seed
	@$(PY) seed/generate.py

seed-extra:  ## sinh thêm dữ liệu cho bài mở rộng trong EXTRA.md (~30 giây)
	@$(PY) seed/generate.py --extra
	@$(PY) tools/explain.py --save-baseline

pipeline:  ## chạy đường ống một lượt (14 ngày vận hành)
	@$(PY) tools/run_pipeline.py

verify:  ## xoá kho, chạy 3 lượt, in bảng chấm — dùng lệnh này liên tục
	@$(PY) tools/verify.py

quick:  ## như verify nhưng chỉ 1 lượt (nhanh, không kiểm tra tính ổn định)
	@$(PY) tools/verify.py --runs 1

explain:  ## [mở rộng] đo rows scanned của queries/dashboard.sql
	@$(PY) tools/explain.py

plan:  ## [mở rộng] explain + in cây EXPLAIN ANALYZE
	@$(PY) tools/explain.py --plan

compact:  ## [mở rộng] chạy tools/compact.py
	@$(PY) tools/compact.py

dbt-test:  ## chạy dbt test
	@$(DBT) test --project-dir dbt --profiles-dir dbt --target-path dbt/target --log-path dbt/logs

dbt-docs:  ## dựng và mở tài liệu dbt (tuỳ chọn)
	@$(DBT) docs generate --project-dir dbt --profiles-dir dbt --target-path dbt/target --log-path dbt/logs
	@$(DBT) docs serve --project-dir dbt --profiles-dir dbt --target-path dbt/target

crash-test:  ## [mở rộng] kịch bản consumer bị giết giữa batch
	@$(PY) tools/crash_test.py

reset:  ## xoá kho DuckDB (giữ nguyên seed và data/)
	@$(PY) -c "import pathlib; [p.unlink(missing_ok=True) for p in map(pathlib.Path,['warehouse.duckdb','warehouse.duckdb.wal'])]"
	@echo "  kho đã xoá."

clean:  ## xoá kho + target dbt + thư mục làm việc của crash-test
	@$(PY) -c "import pathlib,shutil; [shutil.rmtree(x) if x.is_dir() else x.unlink(missing_ok=True) for x in map(pathlib.Path,['warehouse.duckdb','warehouse.duckdb.wal','dbt/target','dbt/logs','data/crash'])]"
	@echo "  đã dọn."
