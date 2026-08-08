#!/usr/bin/env python3
"""Проверяет по Яндекс.Расписаниям маршруты, потерянные при сбое пайплайна.

Зачем. Прогон 07.08.2026 деградировал (AirLabs отдал 163 маршрута вместо 492
при HTTP 200, остальные источники упёрлись в лимиты) и удалил из routes.json
95 маршрутов. Часть из них удалилась законно — после фикса перепутанных
ICAO-кодов рейсы переехали под правильные названия. Остальные надо проверить
независимым источником и вернуть те, что реально существуют.

Что делает: берёт состояние routes.json на указанном коммите, вычитает
текущее — получает список потерянных маршрутов; опрашивает Яндекс.Расписания
по аэропортам затронутых городов и печатает, какие из потерянных маршрутов
источник подтверждает. С --apply дописывает подтверждённые обратно в
routes.json (только добавляет, ничего не удаляет).

Нужен YANDEX_RASP_KEY в окружении — тот же ключ, что у пайплайна.

    YANDEX_RASP_KEY=... python3 scripts/check_lost_routes.py
    YANDEX_RASP_KEY=... python3 scripts/check_lost_routes.py --apply
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
ROUTES_PATH = ROOT / "routes.json"

# Состояние до серии сбойных прогонов 07.08.2026.
DEFAULT_BASE_COMMIT = "c7caa8e"


def routes_at(commit: str) -> dict:
    blob = subprocess.run(["git", "show", f"{commit}:routes.json"],
                          cwd=ROOT, capture_output=True, text=True, check=True).stdout
    return json.loads(blob)


def edges(data: dict) -> set[tuple[str, str]]:
    return {(city, dest) for city, dests in data["routes"].items() for dest in dests}


def apply_confirmed(confirmed: list, base_commit: str) -> None:
    """Дописывает подтверждённые маршруты в routes.json (только добавляет).

    Метаданные направления (координаты, регион, страна) берутся из состояния
    на базовом коммите — там они точно были, раз маршрут существовал.
    """
    current = json.loads(ROUTES_PATH.read_text(encoding="utf-8"))
    base_data = routes_at(base_commit)
    added = 0
    for city, dest in confirmed:
        dests = current["routes"].setdefault(city, [])
        if dest not in dests:
            dests.append(dest)
            added += 1
        if dest not in current["destinations"] and dest in base_data["destinations"]:
            current["destinations"][dest] = base_data["destinations"][dest]
    with ROUTES_PATH.open("w", encoding="utf-8") as f:
        json.dump(current, f, ensure_ascii=False, indent=2)
        f.write("\n")
    total = sum(len(v) for v in current["routes"].values())
    print(f"routes.json обновлён: возвращено {added}, стало {total} маршрутов")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=DEFAULT_BASE_COMMIT,
                    help=f"коммит, с состоянием которого сравнивать (по умолчанию {DEFAULT_BASE_COMMIT})")
    ap.add_argument("--apply", action="store_true",
                    help="вернуть подтверждённые маршруты в routes.json")
    ap.add_argument("--save", metavar="FILE",
                    help="сохранить подтверждённые пары в JSON (чтобы не опрашивать API повторно)")
    ap.add_argument("--apply-from", metavar="FILE",
                    help="вернуть маршруты из ранее сохранённого JSON, без обращения к API")
    args = ap.parse_args()

    # Полный обход занимает десятки минут и заметную часть суточной квоты,
    # поэтому результат проверки можно сохранить (--save) и применить отдельно.
    if args.apply_from:
        apply_confirmed(json.loads(Path(args.apply_from).read_text(encoding="utf-8")), args.base)
        return

    if not os.environ.get("YANDEX_RASP_KEY"):
        raise SystemExit("Нужен YANDEX_RASP_KEY в окружении (тот же, что у пайплайна)")

    # Импорт после проверки ключа: update_routes читает его на уровне модуля.
    import update_routes as ur

    current = json.loads(ROUTES_PATH.read_text(encoding="utf-8"))
    lost = sorted(edges(routes_at(args.base)) - edges(current))
    if not lost:
        print("Потерянных маршрутов нет")
        return

    cities = sorted({city for city, _ in lost})
    print(f"Потеряно {len(lost)} маршрутов у {len(cities)} городов "
          f"(база: {args.base}). Опрашиваем Яндекс.Расписания...\n", flush=True)

    # Опрашиваем только аэропорты затронутых городов — полный обход
    # (78 аэропортов) быстрее упирается в лимит и не нужен.
    airports = [(icao, ur.RU_AIRPORTS[icao])
                for icao, _ in ur.RU_AIRPORTS_ORDERED
                if icao in ur.RU_AIRPORT_IATA and ur.RU_AIRPORTS[icao] in cities]

    found: dict[str, set[str]] = {}
    stopped = False
    for idx, (icao, city) in enumerate(airports, 1):
        iata = ur.RU_AIRPORT_IATA[icao]
        print(f"[{idx}/{len(airports)}] {city} ({iata})", flush=True)
        offset = 0
        while True:
            res = ur._yandex_fetch_page(iata, offset)
            if res is None:
                stopped = True
                break
            items, total = res
            for item in items:
                if (item.get("stops") or "") != "":
                    continue
                dest = ur._yandex_parse_dest(item.get("thread", {}))
                if dest and dest in ur._DEST_NAMES_SET:
                    found.setdefault(city, set()).add(dest)
            offset += ur._YANDEX_PAGE_SIZE
            if offset >= total:
                break
            time.sleep(0.5)
        if stopped:
            print("  Лимит Яндекса — дальше не идём", flush=True)
            break
        time.sleep(1)

    checked_cities = {city for _, city in airports[:idx]} if not stopped else \
                     {city for _, city in airports[:idx - 1]}
    confirmed = [(c, d) for c, d in lost if d in found.get(c, set())]
    unconfirmed = [(c, d) for c, d in lost
                   if c in checked_cities and (c, d) not in confirmed]
    unchecked = [(c, d) for c, d in lost if c not in checked_cities]

    print(f"\nПодтверждено Яндексом: {len(confirmed)}")
    for c, d in confirmed:
        print(f"  + {c} → {d}")
    print(f"\nНе подтверждено ({len(unconfirmed)}) — либо рейса нет, либо его "
          f"нет в расписаниях Яндекса:")
    for c, d in unconfirmed:
        print(f"  - {c} → {d}")
    if unchecked:
        print(f"\nНе проверено ({len(unchecked)}, до этих городов не дошли):")
        for c, d in unchecked:
            print(f"  ? {c} → {d}")

    if args.save:
        Path(args.save).write_text(json.dumps(confirmed, ensure_ascii=False, indent=2),
                                   encoding="utf-8")
        print(f"\nПодтверждённые сохранены в {args.save}")

    if not args.apply:
        print("\nЗапустите с --apply, чтобы вернуть подтверждённые в routes.json")
        return
    if not confirmed:
        print("\nВозвращать нечего")
        return
    apply_confirmed(confirmed, args.base)


if __name__ == "__main__":
    main()
