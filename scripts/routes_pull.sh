#!/bin/sh
# Ежечасный pull routes.json из GitHub на прод russia-flights.ru.
#
# Живёт на хостинге в ~/russia-flights/routes_pull.sh (ВНЕ public_html —
# снаружи недоступен), вызывается из cron Timeweb, который правится только
# через Панель Управления Аккаунтом (команда crontab на хостинге отключена).
# Источник истины — этот файл в репозитории; на хостинг попадает ручным scp.
#
# Зачем не просто `curl -o routes.json <url>`: обрыв на середине, 5xx или
# HTML-страница ошибки вместо JSON перезаписали бы боевой файл мусором, а
# читатель мог застать файл недописанным. Здесь: временный файл -> проверка,
# что это валидный JSON -> атомарный mv (tmp в том же $HOME, что и цель,
# поэтому mv не копирует, а переименовывает).
set -u

URL="https://raw.githubusercontent.com/xkonstantinnnx/russia-flights/main/routes.json"
DEST="$HOME/russia-flights/public_html/routes.json"
TMP="$HOME/russia-flights/routes.json.tmp"

curl --fail --silent --show-error --max-time 60 -o "$TMP" "$URL" || { rm -f "$TMP"; exit 1; }

# Валидация JSON: python3, иначе php (на shared-хостинге есть всегда),
# иначе хотя бы проверка, что файл начинается с "{"
if command -v python3 >/dev/null 2>&1; then
  python3 -m json.tool "$TMP" >/dev/null 2>&1 || { rm -f "$TMP"; exit 1; }
elif command -v php >/dev/null 2>&1; then
  php -r 'exit(json_decode(file_get_contents($argv[1])) === null ? 1 : 0);' "$TMP" || { rm -f "$TMP"; exit 1; }
else
  [ "$(head -c1 "$TMP")" = "{" ] || { rm -f "$TMP"; exit 1; }
fi

mv -f "$TMP" "$DEST"
