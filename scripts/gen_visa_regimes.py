#!/usr/bin/env python3
"""Генерирует VISA_REGIMES для index.html — визовый режим для граждан РФ
по каждой стране из DEST_INFO.c (без визы / e-виза-по прилёту / виза заранее).

Источник — ru.wikipedia.org, статья «Визовые требования для граждан
России»: живая, регулярно правящаяся таблица, каждая строка ссылается на
первоисточник (обычно kdmid.ru/mid.ru или профильные новости). Датасеты
вроде passport-index-dataset сознательно не используются как первичный
источник — устаревают быстрее (проверено вручную: там Китай всё ещё
"виза требуется", хотя с 15.09.2025 действует пробный безвизовый режим
для туристов РФ — статья это отражает, датасет нет).

ВНИМАНИЕ: не часть автопайплайна (update.yml). Визовые режимы меняются
редко и внезапно — запускать вручную раз в несколько месяцев, глазами
сверяя вывод в stderr (особенно топ направления по числу маршрутов)
перед тем как коммитить сгенерированный index.html.
"""
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ROUTES_PATH = ROOT / "routes.json"
INDEX_PATH = ROOT / "index.html"

WIKI_TITLE = "Визовые_требования_для_граждан_России"
WIKI_URL = f"https://ru.wikipedia.org/w/index.php?title={urllib.parse.quote(WIKI_TITLE)}&action=raw"
WIKI_ARTICLE_URL = "https://ru.wikipedia.org/wiki/Визовые_требования_для_граждан_России"

# DEST_INFO.c (как в routes.json) -> название страны в статье (шаблон {{флаг|X}})
COUNTRY_TO_WIKI = {
    "Абхазия": "Абхазия",
    "Азербайджан": "Азербайджан",
    "Алжир": "Алжир",
    "Армения": "Армения",
    "Бахрейн": "Бахрейн",
    "Беларусь": "Белоруссия",
    "Вьетнам": "Вьетнам",
    "Гонконг (КНР)": "Гонконг",
    "Грузия": "Грузия",
    "Египет": "Египет",
    "Израиль": "Израиль",
    "Индия": "Индия",
    "Индонезия (Бали)": "Индонезия",
    "Иордания": "Иордания",
    "Иран": "Иран",
    "КНДР": "Северная Корея",
    "Казахстан": "Казахстан",
    "Катар": "Катар",
    "Китай": "Китай",
    "Кувейт": "Кувейт",
    "Кыргызстан": "Киргизия",
    "Мальдивы": "Мальдивы",
    "Марокко": "Марокко",
    "Монголия": "Монголия",
    "ОАЭ": "ОАЭ",
    "Оман": "Оман",
    "Саудовская Аравия": "Саудовская Аравия",
    "Сейшелы": "Сейшельские острова",
    "Сербия": "Сербия",
    "Таджикистан": "Таджикистан",
    "Таиланд": "Таиланд",
    "Танзания": "Танзания",
    "Тунис": "Тунис",
    "Туркменистан": "Туркменистан",
    "Турция": "Турция",
    "Узбекистан": "Узбекистан",
    "Шри-Ланка": "Шри-Ланка",
    "Эфиопия": "Эфиопия",
}

# Точечные ручные поправки — авто-классификация эвристическая (свободный
# текст, неоднородный формат ячеек в статье) и вручную сверена по всем 38
# строкам 2026-08-05, а не только по топ-10. Причины:
MANUAL_OVERRIDES: dict[str, str] = {
    # безвизовый режим по тексту статьи, эвристика не поймала характерную
    # фразу (либо её нет в первых 400 символах, либо перекрыта другим срабатыванием)
    "Абхазия": "free",           # союзнический режим РФ-Абхазия, фактически открытая граница
    "Азербайджан": "free",       # 90 дней, безвизовый режим СНГ
    "Гонконг (КНР)": "free",     # 14 дней безвизово (одностороннее решение Гонконга)
    "Иордания": "free",          # межправсоглашение об отмене виз, 30 дней
    "Казахстан": "free",         # 90 дней, безвизовый режим СНГ
    "Катар": "free",             # межправсоглашение об отмене виз, 90 дней
    "Кыргызстан": "free",        # 90 дней, безвизовый режим СНГ
    "Мальдивы": "free",          # бесплатный штамп по прилёту, по факту безвизово
    "Марокко": "free",           # безвизовый режим, 90 дней
    "Монголия": "free",          # безвизовый режим
    "Саудовская Аравия": "free", # межправсоглашение "об отмене визовых требований"
    "Сербия": "free",            # давний безвизовый режим для граждан РФ
    "Таиланд": "free",           # "туризм: 60 дней (без сборов)" — прямо безвизово
    "Тунис": "free",             # "3 месяца безвизового пребывания"
    "Узбекистан": "free",        # "виза не требуется, срок пребывания не ограничен"
    # Индонезия: безвизово до 30 дней (обычный туризм); виза по прибытии —
    # только при более долгом/ином визите, для сайта релевантен базовый случай
    "Индонезия (Бали)": "free",
    # индикатор "нет"/текст про предварительную визу; фраза "по прибытии"
    # относится к узкому исключению (для резидентов ряда стран Залива),
    # эвристика ошибочно сработала по этому слову для общего случая
    "Кувейт": "visa",
    # текст про безвизовый режим касается только организованных тургрупп
    # 5-50 человек — для типичного индивидуального туриста нужна виза/e-visa,
    # поэтому явно фиксируем "easy", а не "free" по общей фразе в статье
    "Иран": "easy",
    # разметка ячейки в статье нестандартная (без {{да|}}/{{нет|}} обёртки,
    # для этой строки авто-извлечение цепляет соседнюю колонку) — по факту
    # у Индии электронная виза (ETA), категория "easy" верна, фиксируем явно
    "Индия": "easy",
    # та же нестандартная разметка, что и у Индии; по факту у Шри-Ланки
    # бесплатная электронная ETA для граждан РФ
    "Шри-Ланка": "free",
}

FREE_HINTS = ["безвизов", "виза не требуется", "визы не требуется", "без визы"]
EASY_HINTS = ["по прибытии", "по прилёте", "по прилете", "электронн", "e-visa", "evisa", "eta"]


def fetch_wikitext() -> str:
    req = urllib.request.Request(WIKI_URL, headers={"User-Agent": "russia-flights-visa-script/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8")


def extract_country_block(wikitext: str, country: str) -> str | None:
    marker = "{{флаг|%s}}" % country
    idx = wikitext.find(marker)
    if idx == -1:
        return None
    end = wikitext.find("\n|-", idx)
    return wikitext[idx:end] if end != -1 else wikitext[idx : idx + 2000]


def classify(block: str) -> tuple[str, str]:
    """Возвращает (категория, сниппет для контроля глазами в stderr)."""
    m_ind = re.search(r"!\{\{(yes2?|no|partial)\|([^}]*)\}\}", block)
    indicator = m_ind.group(1) if m_ind else "?"

    m_detail = re.search(r"\{\{(да|нет)\|(.+)", block, re.DOTALL)
    if not m_detail:
        return "visa", f"[индикатор={indicator}, детальная ячейка не найдена — перестраховка]"

    detail_tag = m_detail.group(1)
    detail_text = m_detail.group(2)[:400].lower()

    if any(h in detail_text for h in EASY_HINTS):
        cat = "easy"
    elif detail_tag == "да" and any(h in detail_text for h in FREE_HINTS):
        cat = "free"
    elif detail_tag == "да":
        # "да" (виза не нужна) без явного "безвизов" в начале текста —
        # перестраховываемся в сторону easy, а не free; ручная сверка
        # топ-направлений всё равно обязательна перед коммитом
        cat = "easy"
    else:
        cat = "visa"
    return cat, f"[индикатор={indicator}] {detail_text[:150]}"


def build_visa_regimes() -> dict:
    wikitext = fetch_wikitext()
    result = {}
    for c_label, wiki_name in COUNTRY_TO_WIKI.items():
        if c_label in MANUAL_OVERRIDES:
            result[c_label] = MANUAL_OVERRIDES[c_label]
            print(f"{c_label:28s} -> {result[c_label]:5s} [override]", file=sys.stderr)
            continue
        block = extract_country_block(wikitext, wiki_name)
        if block is None:
            print(f"{c_label:28s} -> !!! НЕ НАЙДЕНО ({wiki_name}) — нужен MANUAL_OVERRIDES", file=sys.stderr)
            result[c_label] = "visa"
            continue
        cat, why = classify(block)
        result[c_label] = cat
        print(f"{c_label:28s} -> {cat:5s} {why}", file=sys.stderr)
    return result


def replace_between(text: str, start_marker: str, end_marker: str, new_content: str) -> str:
    pattern = re.compile(re.escape(start_marker) + r".*?" + re.escape(end_marker), re.DOTALL)
    if not pattern.search(text):
        raise SystemExit(f"Маркеры {start_marker!r}/{end_marker!r} не найдены в index.html")
    replacement = f"{start_marker}\n{new_content}\n{end_marker}"
    return pattern.sub(lambda _m: replacement, text, count=1)


def build_js_block(regimes: dict) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    lines = [
        "// Визовый режим для граждан РФ: free = без визы, easy = e-виза/по прилёту,",
        "// visa = нужна виза заранее. Ключи — DEST_INFO.c из routes.json.",
        f"// Источник: {WIKI_ARTICLE_URL} (сверено {now}). Не автообновляется —",
        "// см. scripts/gen_visa_regimes.py, визовые правила меняются без предупреждения.",
        "const VISA_REGIMES = " + json.dumps(regimes, ensure_ascii=False, indent=2) + ";",
    ]
    return "\n".join(lines)


def main():
    routes_data = json.loads(ROUTES_PATH.read_text(encoding="utf-8"))
    countries_in_use = sorted(set(v["c"] for v in routes_data["destinations"].values()))
    missing = set(countries_in_use) - set(COUNTRY_TO_WIKI.keys())
    if missing:
        raise SystemExit(f"В routes.json появились новые страны без маппинга: {missing}")

    regimes = build_visa_regimes()
    html = INDEX_PATH.read_text(encoding="utf-8")
    html = replace_between(html, "// VISA_REGIMES_START", "// VISA_REGIMES_END", build_js_block(regimes))
    INDEX_PATH.write_text(html, encoding="utf-8")
    print(f"\nOK: index.html обновлён, {len(regimes)} стран", file=sys.stderr)


if __name__ == "__main__":
    main()
