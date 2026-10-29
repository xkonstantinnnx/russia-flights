#!/bin/sh
# Ежечасный pull из GitHub на прод russia-flights.ru: routes.json, а также
# sitemap.xml и index.html, которые пайплайн перегенерирует вместе с маршрутами.
#
# Живёт на хостинге в ~/russia-flights/routes_pull.sh (ВНЕ public_html —
# снаружи недоступен), вызывается из cron Timeweb, который правится только
# через Панель Управления Аккаунтом (команда crontab на хостинге отключена).
# Источник истины — этот файл в репозитории; на хостинг попадает ручным scp.
#
# Зачем не просто `curl -o routes.json <url>`: обрыв на середине, 5xx или
# HTML-страница ошибки вместо JSON перезаписали бы боевой файл мусором, а
# читатель мог застать файл недописанным. Здесь: временный файл -> проверка
# содержимого -> атомарный mv (tmp в том же $HOME, что и цель, поэтому mv не
# копирует, а переименовывает). Файлы независимы: сбой одного не мешает другим.
#
# index.html забирается, только если номер версии в нём тот же, что на проде.
# Пайплайн версию не трогает, поэтому свежие данные уезжают сами, а ручная
# правка (по правилам проекта она всегда с новым номером версии) ждёт ручного
# деплоя через scp — после него версии снова совпадут и pull продолжится.
set -u

BASE="https://raw.githubusercontent.com/xkonstantinnnx/russia-flights/main"
WORK="$HOME/russia-flights"
WEB="$WORK/public_html"
rc=0

# fetch NAME: скачать NAME из репозитория в $WORK/NAME.tmp
fetch() {
  curl --fail --silent --show-error --max-time 60 -o "$WORK/$1.tmp" "$BASE/$1" || { rm -f "$WORK/$1.tmp"; return 1; }
}

# install_if_changed NAME: положить на место, только если содержимое другое —
# иначе Last-Modified менялся бы каждый час и роботы видели бы правку без правки
install_if_changed() {
  if [ -f "$WEB/$1" ] && cmp -s "$WORK/$1.tmp" "$WEB/$1"; then
    rm -f "$WORK/$1.tmp"
  else
    mv -f "$WORK/$1.tmp" "$WEB/$1"
  fi
}

# Номера версии из бейджа и из «О проекте»: ">v4.4" в двух местах
versions() {
  grep -o '>v[0-9][0-9]*\.[0-9][0-9]*' "$1" 2>/dev/null | sort -u | tr '\n' ' '
}

# routes.json перезаписывается каждый час без сравнения: его Last-Modified —
# признак того, что cron жив (curl -I https://russia-flights.ru/routes.json)
if fetch routes.json; then
  # Валидация JSON: python3, иначе php (на shared-хостинге есть всегда),
  # иначе хотя бы проверка, что файл начинается с "{"
  T="$WORK/routes.json.tmp"
  if command -v python3 >/dev/null 2>&1; then
    python3 -m json.tool "$T" >/dev/null 2>&1 || { rm -f "$T"; rc=1; }
  elif command -v php >/dev/null 2>&1; then
    php -r 'exit(json_decode(file_get_contents($argv[1])) === null ? 1 : 0);' "$T" || { rm -f "$T"; rc=1; }
  else
    [ "$(head -c1 "$T")" = "{" ] || { rm -f "$T"; rc=1; }
  fi
  [ -f "$T" ] && mv -f "$T" "$WEB/routes.json"
else
  rc=1
fi

if fetch sitemap.xml; then
  T="$WORK/sitemap.xml.tmp"
  if grep -q '<urlset' "$T" && grep -q '</urlset>' "$T"; then
    install_if_changed sitemap.xml
  else
    rm -f "$T"; rc=1
  fi
else
  rc=1
fi

if fetch index.html; then
  T="$WORK/index.html.tmp"
  NEW_V=$(versions "$T")
  if [ "$(wc -c < "$T")" -lt 100000 ] || ! grep -q '</html>' "$T" \
     || ! grep -q 'const RU_COORDS' "$T" || [ -z "$NEW_V" ]; then
    rm -f "$T"; rc=1
  elif [ "$NEW_V" != "$(versions "$WEB/index.html")" ]; then
    # Другая версия — ручная правка, её выкладывают руками. Не ошибка.
    rm -f "$T"
  else
    install_if_changed index.html
  fi
else
  rc=1
fi

exit $rc
