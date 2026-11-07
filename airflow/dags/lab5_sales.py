"""Лабораторна 5. Конвеєр із трьох кроків: extract -> transform -> publish."""
import csv
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task, get_current_context

# ===== НАЛАШТУВАННЯ ВАРІАНТА (змінюйте лише цей блок) =====
REGION = "West"                  # регіон, який обробляє конвеєр
FAIL_AT = None                   # крок, який «ламаємо»: "extract", "transform", "publish" або None
FLAKY_FAILS = 0                  # скільки перших спроб extract «падає» (імітація збою мережі); 0 = не падає
RETRIES = 0                      # скільки повторних спроб дозволено
SCHEDULE = None                  # None = лише ручний запуск; "@daily" = щодня
DAYS_BACK = 3                    # з якого дня назад починається розклад (для backfill)
CATCHUP = False                  # True: Airflow сам запускає пропущені дні
# ==========================================================

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "warehouse_flat_clean.csv"
OUT = ROOT / "output"


@dag(
    dag_id="lab5_sales",
    schedule=SCHEDULE,
    start_date=pendulum.today("UTC").subtract(days=DAYS_BACK),
    catchup=CATCHUP,
    default_args={"retries": RETRIES, "retry_delay": timedelta(seconds=10)},
    tags=["lab5"],
)
def lab5_sales():
    @task
    def extract():
        ctx = get_current_context()
        attempt = ctx["ti"].try_number
        print(f"extract: спроба № {attempt}")
        if FAIL_AT == "extract":
            raise ValueError("Збій у кроці extract (навмисний)")
        if attempt <= FLAKY_FAILS:
            raise ConnectionError("Тимчасовий збій мережі (імітація)")
        OUT.mkdir(exist_ok=True)
        with open(DATA, encoding="utf-8") as f:
            rows = [r for r in csv.DictReader(f) if r["Region"] == REGION]
        with open(OUT / "raw.csv", "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["Order Date", "Sales", "Profit"])
            w.writeheader()
            for r in rows:
                w.writerow({k: r[k] for k in ("Order Date", "Sales", "Profit")})
        print(f"extract: регіон {REGION}, рядків {len(rows)}")

    @task
    def transform():
        if FAIL_AT == "transform":
            raise ValueError("Збій у кроці transform (навмисний)")
        agg = {}
        with open(OUT / "raw.csv", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                m = r["Order Date"][:7]
                s, p = agg.get(m, (0.0, 0.0))
                agg[m] = (s + float(r["Sales"]), p + float(r["Profit"]))
        with open(OUT / "mart.csv", "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["month", "sales", "profit"])
            for m in sorted(agg):
                w.writerow([m, round(agg[m][0], 2), round(agg[m][1], 2)])
        print(f"transform: місяців у вітрині {len(agg)}")

    @task
    def publish():
        if FAIL_AT == "publish":
            raise ValueError("Збій у кроці publish (навмисний)")
        with open(OUT / "mart.csv", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        total = round(sum(float(r["sales"]) for r in rows), 2)
        text = f"Регіон {REGION}: місяців {len(rows)}, продажі {total}\n"
        (OUT / "report.txt").write_text(text, encoding="utf-8")
        print("publish:", text.strip())

    t1, t2, t3 = extract(), transform(), publish()
    t1 >> t2 >> t3


lab5_sales()
