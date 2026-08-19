# -*- coding: utf-8 -*-
"""
Скринер открытого интереса — LIVE (валютные фьючерсы MOEX).

Локальный сервер + веб-интерфейс в стиле mscinsider:
настраиваемая сетка до 8 окон, в каждом — свой инструмент, Юр./Физ.,
Покупки или Продажи, таймфрейм, линия/свечи, зум и прокрутка.

• Токен берётся из algopack_token.txt (рядом с этим файлом) — НЕ попадает в браузер.
• Токен принят MOEX -> данные LIVE (apim.moex.com).
• Токен отвергнут/просрочен -> автоматически бесплатные данные с задержкой 14 дн.
• Сервер слушает 0.0.0.0 -> скринер виден с телефона/других устройств в той же сети.

Запуск: двойной клик по Скринер_LIVE.bat  (или `py -3 oi_live.py`).
Стоп: закрыть окно консоли или Ctrl+C.
"""
import json, ssl, time, threading, webbrowser, datetime, os, sys, socket, subprocess
import urllib.request, urllib.error
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
TOKEN_FILE = os.path.join(HERE, "algopack_token.txt")
PORT = int(os.environ.get("PORT", "8777"))            # облако (Render и т.п.) задаёт порт через $PORT
ACCESS_KEY = os.environ.get("ACCESS_KEY", "").strip()  # если задан — доступ только по ?key=... (замок)

APIM = "https://apim.moex.com/iss/analyticalproducts/futoi/securities/{t}.json?iss.meta=off&from={f}&till={d}"
FREE = "https://iss.moex.com/iss/analyticalproducts/futoi/securities/{t}.json?iss.meta=off&from={f}&till={d}"

INSTRUMENTS = ["CR", "Si", "Eu", "ED", "CNYRUBF", "USDRUBF", "EURRUBF", "SS", "NR"]

_ctx = ssl.create_default_context()
_lock = threading.Lock()
_cache = {}             # (ticker, frm, till) -> (ts, payload)
_auth_fail_until = 0.0  # после 401 не дёргаем apim 120 c
CACHE_TTL = 25.0


def read_token():
    env = os.environ.get("ALGOPACK_TOKEN", "").strip()   # облако: токен в переменной окружения (не в репозитории)
    if env:
        return env
    try:
        with open(TOKEN_FILE, encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""


def http_get(url, token=None, timeout=5, tries=2):
    """Запрос с коротким таймаутом и одним быстрым повтором.
    При параллельных запросах apim иногда подвисает на одном соединении — короткий
    таймаут (5 c) обрывает зависшее, повтор обычно проходит за <1 c."""
    req = urllib.request.Request(url, headers={
        "User-Agent": "oi_live/3.0", "Accept": "application/json"})
    if token:
        req.add_header("Authorization", "Bearer " + token)
    last = (0, "init")
    for attempt in range(tries):
        for ctx in (_ctx, ssl._create_unverified_context()):
            try:
                with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
                    return r.status, r.read().decode("utf-8", "replace")
            except urllib.error.HTTPError as e:           # 401/403/4xx — не повторяем
                body = ""
                try:
                    body = e.read().decode("utf-8", "replace")
                except Exception:
                    pass
                return e.code, body
            except ssl.SSLError:                          # пробуем без проверки сертификата
                continue
            except Exception as e:                        # сетевой сбой/таймаут — повтор
                last = (0, str(e))
                break
        if attempt + 1 < tries:
            time.sleep(0.3)
    return last


def parse_futoi(body):
    try:
        j = json.loads(body)
    except Exception:
        return None
    f = j.get("futoi")
    if not f or not f.get("columns"):
        return None
    cols = f["columns"]
    if cols and cols[0] == "ERROR_MESSAGE":
        return {"error": ((f.get("data") or [["ISS error"]])[0] or ["ISS error"])[0]}
    return {"columns": cols, "data": f.get("data") or []}


def _iso(d):
    return d.isoformat()


def _trading_days(dfrom, dtill):
    """Даты пн–пт в диапазоне [dfrom..dtill] (выходные пропускаем; праздники вернут пусто)."""
    try:
        d0 = datetime.date.fromisoformat(dfrom)
        d1 = datetime.date.fromisoformat(dtill)
    except Exception:
        return []
    if d1 < d0:
        d0, d1 = d1, d0
    out, d = [], d0
    while d <= d1:
        if d.weekday() < 5:
            out.append(d)
        d += datetime.timedelta(days=1)
    return out


_day_cache = {}  # (ticker, day) -> (ts, payload)


def fetch_day(ticker, day_iso):
    """Один торговый день. apim LIVE → при отказе бесплатно (только для дней старше 14 дн).
    Прошлые дни неизменны → кэш надолго; сегодня — 25 c. Обходит лимит ISS 1000 строк/запрос."""
    global _auth_fail_until
    today = datetime.date.today()
    ttl = CACHE_TTL if day_iso == today.isoformat() else 3600.0
    key = (ticker, day_iso)
    now = time.time()
    with _lock:
        ent = _day_cache.get(key)
        if ent and now - ent[0] < ttl:
            return ent[1]

    payload = None
    token = read_token()
    if token and time.time() >= _auth_fail_until:
        st, body = http_get(APIM.format(t=ticker, f=day_iso, d=day_iso), token)
        if st in (401, 403):
            with _lock:
                _auth_fail_until = time.time() + 120
        else:
            p = parse_futoi(body)
            if p and not p.get("error") and p.get("data"):
                payload = {"source": "live", "columns": p["columns"], "data": p["data"]}
    if payload is None:
        st, body = http_get(FREE.format(t=ticker, f=day_iso, d=day_iso))
        p = parse_futoi(body)
        if p and not p.get("error") and p.get("data"):
            lag = (today - datetime.date.fromisoformat(day_iso)).days
            payload = {"source": "delayed", "lag": lag,
                       "columns": p["columns"], "data": p["data"]}

    if payload is not None:           # кэшируем только успех; промахи (выходной/праздник) — нет
        with _lock:
            _day_cache[key] = (now, payload)
    return payload


def fetch_ticker(ticker, dfrom, dtill):
    """Диапазон [dfrom..dtill] по дням — обходит лимит ISS 1000 строк, поэтому Неделя/Месяц
    показываются целиком. LIVE через apim; если ничего нет — бесплатный фолбэк с задержкой."""
    cols, rows, srcs, lag = None, [], set(), None
    for d in _trading_days(dfrom, dtill):
        dp = fetch_day(ticker, _iso(d))
        if dp and dp.get("data"):
            cols = cols or dp["columns"]
            rows.extend(dp["data"])
            srcs.add(dp["source"])
            if dp.get("lag") is not None:
                lag = dp["lag"]
    if rows:
        source = "live" if "live" in srcs else "delayed"
        return {"source": source, "frm": dfrom, "till": dtill, "ticker": ticker,
                "lag": (None if source == "live" else lag), "columns": cols, "data": rows}

    # ничего не пришло (нет токена / транзиентный 401) → показать задержанные данные
    return _delayed_fallback(ticker, max(1, len(_trading_days(dfrom, dtill))))


def _delayed_fallback(ticker, ndays):
    """Свежайшие доступные бесплатные дни (≈ сегодня−15 и раньше), ndays торговых дней."""
    today = datetime.date.today()
    collected, d, guard = [], today - datetime.timedelta(days=15), 0
    while len(collected) < ndays and guard < 45:
        if d.weekday() < 5:
            dp = fetch_day(ticker, _iso(d))
            if dp and dp.get("data"):
                collected.append((d, dp))
        d -= datetime.timedelta(days=1)
        guard += 1
    if not collected:
        return {"source": "error", "ticker": ticker,
                "message": "нет данных или токен отвергнут", "columns": [], "data": []}
    collected.sort(key=lambda x: x[0])
    cols, rows, lag = None, [], None
    for _d, dp in collected:
        cols = cols or dp["columns"]
        rows.extend(dp["data"])
        lag = dp.get("lag", lag)
    return {"source": "delayed", "frm": _iso(collected[0][0]), "till": _iso(collected[-1][0]),
            "lag": lag, "ticker": ticker, "columns": cols, "data": rows}


def fetch_all(dfrom, dtill):
    # 1) греем кэш всех (тикер, день) одним пулом — параллельно по дням И тикерам
    day_isos = [_iso(d) for d in _trading_days(dfrom, dtill)]
    tasks = [(t, di) for t in INSTRUMENTS for di in day_isos]
    if tasks:
        with ThreadPoolExecutor(max_workers=min(8, len(tasks))) as ex:
            list(ex.map(lambda td: fetch_day(*td), tasks))
    # 2) собираем по тикерам (теперь почти всё из кэша; сеть только для фолбэка)
    out = {}
    with ThreadPoolExecutor(max_workers=len(INSTRUMENTS)) as ex:
        for t, payload in zip(INSTRUMENTS,
                              ex.map(lambda t: fetch_ticker(t, dfrom, dtill), INSTRUMENTS)):
            out[t] = payload
    return {"instruments": INSTRUMENTS, "data": out, "token": bool(read_token())}


# ── AlgoPack: продукты по контракту (tradestats «поток», hi2 «концентрация») ──────
# Отдельные окна берут эти данные ПО КОНКРЕТНОМУ контракту, поэтому для квартальных
# (CR/Si/Eu/ED) надо знать активный фронт-контракт; вечные (…RUBF) = сами себе.
ALGO = ("https://apim.moex.com/iss/datashop/algopack/fo/{prod}/{sec}.json"
        "?iss.meta=off&from={f}&till={d}")
FORTS_SEC = "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json?iss.meta=off"
FRONT_MAP = {"CR": ("CNY", "CR"), "Si": ("Si", "Si"),   # futoi-код -> (ASSETCODE в FORTS, префикс SECID)
             "Eu": ("Eu", "Eu"), "ED": ("ED", "ED"),
             "SS": ("SMLT", "SS"), "NR": ("NGM", "NR")}  # акции: Самолёт (фронт SSU6), NGM (фронт NRN6)
PERP = {"CNYRUBF", "USDRUBF", "EURRUBF"}
_fronts = {}              # code -> (ts, secid)
_series_cache = {}        # (src, code, frm, till) -> (ts, payload)
FRONT_TTL = 6 * 3600.0    # фронт меняется только на ролловере (раз в квартал)


def resolve_front(code):
    """SECID активного контракта для futoi-кода. Вечный = сам; квартальный = макс OPENPOSITION
    среди контрактов нужного ASSETCODE (юань на FORTS = 'CNY', SECID начинается на 'CR')."""
    if code in PERP:
        return code
    now = time.time()
    with _lock:
        ent = _fronts.get(code)
        if ent and now - ent[0] < FRONT_TTL:
            return ent[1]
    asset, prefix = FRONT_MAP.get(code, (code, code))
    secid = None
    st, body = http_get(FORTS_SEC, timeout=6, tries=2)
    try:
        j = json.loads(body)
        sec, md = j.get("securities", {}), j.get("marketdata", {})
        sc, sd = sec.get("columns", []), sec.get("data", [])
        iSID, iAS = sc.index("SECID"), sc.index("ASSETCODE")
        op = {}
        mc, mdd = md.get("columns", []), md.get("data", [])
        if "SECID" in mc and "OPENPOSITION" in mc:
            jS, jO = mc.index("SECID"), mc.index("OPENPOSITION")
            for r in mdd:
                op[r[jS]] = r[jO] or 0
        best = -1
        for r in sd:
            sid, a = r[iSID], r[iAS]
            if a == asset and str(sid).startswith(prefix):
                v = op.get(sid, 0) or 0
                if v > best:
                    best, secid = v, sid
    except Exception:
        secid = None
    if secid:
        with _lock:
            _fronts[code] = (now, secid)
    return secid


def _algo_paged(prod, sec, frm, till, token):
    """Все строки продукта AlgoPack (блок ответа называется 'data') с учётом курсор-пагинации.
    Возвращает (columns, rows) или (None, причина)."""
    rows, cols, start, guard = [], None, 0, 0
    while guard < 40:
        guard += 1
        url = ALGO.format(prod=prod, sec=sec, f=frm, d=till)
        if start:
            url += "&start=%d" % start
        st, body = http_get(url, token, timeout=8, tries=2)
        if st in (401, 403):
            return None, "auth"
        try:
            j = json.loads(body)
        except Exception:
            break
        b = j.get("data") or {}
        c, d = b.get("columns"), b.get("data")
        if c and cols is None:
            cols = c
        if d:
            rows.extend(d)
        cur = j.get("data.cursor") or {}
        cc, cd = cur.get("columns"), (cur.get("data") or [])
        if cc and cd:
            try:
                iI, iT, iP = cc.index("INDEX"), cc.index("TOTAL"), cc.index("PAGESIZE")
                idx, tot, ps = cd[0][iI], cd[0][iT], cd[0][iP]
                if ps and idx + ps < tot:
                    start = idx + ps
                    continue
            except Exception:
                pass
        break
    if cols is None:
        return None, "empty"
    return cols, rows


def fetch_flow(code, frm, till):
    """tradestats по фронт-контракту: ОИ контракта + объёмы покупок/продаж (агрессоры)."""
    sec = resolve_front(code)
    base = {"src": "flow", "code": code, "secid": sec, "columns": [], "data": []}
    if not sec:
        return dict(base, source="error", message="контракт не найден")
    token = read_token()
    if not token:
        return dict(base, source="error", message="нужен токен AlgoPack")
    cols, rows = _algo_paged("tradestats", sec, frm, till, token)
    if not cols or rows == "auth":
        return dict(base, source="error", message="нет данных (токен?)")
    ix = {n: (cols.index(n) if n in cols else None)
          for n in ("tradedate", "tradetime", "oi_close", "vol_b", "vol_s", "pr_close")}
    out = []
    for r in rows:
        g = lambda k: (None if ix[k] is None else r[ix[k]])
        out.append([g("tradedate"), g("tradetime"), g("oi_close"),
                    g("vol_b"), g("vol_s"), g("pr_close")])
    return dict(base, source="live", interval=5,
                columns=["date", "time", "oi", "vb", "vs", "close"], data=out)


def fetch_conc(code, frm, till):
    """hi2 по фронт-контракту: индексы концентрации HHI (один игрок доминирует в потоке?).
    Сырой формат длинный (строка на метрику) -> разворачиваем в широкий (метрики по столбцам)."""
    sec = resolve_front(code)
    base = {"src": "conc", "code": code, "secid": sec, "columns": [], "data": []}
    if not sec:
        return dict(base, source="error", message="контракт не найден")
    token = read_token()
    if not token:
        return dict(base, source="error", message="нужен токен AlgoPack")
    cols, rows = _algo_paged("hi2", sec, frm, till, token)
    if not cols or rows == "auth":
        return dict(base, source="error", message="нет данных (токен?)")
    try:
        iD, iT = cols.index("tradedate"), cols.index("tradetime")
        iM, iV = cols.index("metric"), cols.index("value")
    except ValueError:
        return dict(base, source="error", message="формат hi2")
    piv, seen = {}, []
    for r in rows:
        m = r[iM]
        piv.setdefault((r[iD], r[iT]), {})[m] = r[iV]
        if m not in seen:
            seen.append(m)
    show = [m for m in ("hhi_agressive", "hhi_buy", "hhi_sell", "hhi_passive", "hhi_volume")
            if m in seen]
    data = [[d, t] + [piv[(d, t)].get(m) for m in show] for (d, t) in sorted(piv.keys())]
    return dict(base, source="live", metrics=show,
                columns=["date", "time"] + show, data=data)


def get_series(src, code, frm, till):
    """Кэш на CACHE_TTL (как у futoi): автообновление каждые 45 c не дёргает apim лишний раз."""
    key = (src, code, frm, till)
    now = time.time()
    with _lock:
        ent = _series_cache.get(key)
        if ent and now - ent[0] < CACHE_TTL:
            return ent[1]
    res = fetch_flow(code, frm, till) if src == "flow" else fetch_conc(code, frm, till)
    if res.get("source") != "error":
        with _lock:
            _series_cache[key] = (now, res)
    return res


def _clean_date(s, default):
    try:
        datetime.date.fromisoformat(s)
        return s
    except Exception:
        return default


GATE_HTML = ("<!doctype html><meta charset=utf-8>"
             "<title>Скринер ОИ</title>"
             "<body style='background:#06090d;color:#d6dee8;font-family:system-ui;margin:0;"
             "display:flex;height:100vh;align-items:center;justify-content:center'>"
             "<div style='text-align:center'><div style='font-size:42px'>🔒</div>"
             "<h2>Скринер открытого интереса</h2>"
             "<p style='color:#6b7888'>Добавь к адресу <code>?key=ТВОЙ_КЛЮЧ</code></p></div>")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, ctype, body, cookie=None):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            pass

    def _gate(self, q):
        """'ok' / 'login' / 'deny'. Пустой ACCESS_KEY = замок выключен (локальный режим)."""
        if not ACCESS_KEY:
            return "ok"
        if (q.get("key") or [""])[0] == ACCESS_KEY:
            return "login"                       # валидный ключ в URL -> поставим cookie и уберём его из адреса
        ck = self.headers.get("Cookie", "") or ""
        for p in ck.split(";"):
            p = p.strip()
            if p.startswith("oi_key=") and p[7:] == ACCESS_KEY:
                return "ok"
        return "deny"

    def do_GET(self):
        try:
            u = urlparse(self.path)
            q = parse_qs(u.query)
            g = self._gate(q)
            if g == "login":
                self.send_response(302)
                self.send_header("Set-Cookie", "oi_key=%s; Path=/; Max-Age=31536000; SameSite=Lax" % ACCESS_KEY)
                self.send_header("Location", u.path or "/")
                self.end_headers()
                return
            if g == "deny":
                self._send(401, "text/html; charset=utf-8", GATE_HTML)
                return
            today = _iso(datetime.date.today())
            if u.path in ("/", "/index.html"):
                self._send(200, "text/html; charset=utf-8", HTML)
            elif u.path == "/api/all":
                frm = _clean_date((q.get("from") or [today])[0], today)
                till = _clean_date((q.get("till") or [today])[0], today)
                self._send(200, "application/json; charset=utf-8",
                           json.dumps(fetch_all(frm, till)))
            elif u.path == "/api/futoi":
                t = (q.get("ticker") or [""])[0]
                if t not in INSTRUMENTS:
                    self._send(400, "application/json",
                               json.dumps({"source": "error", "message": "bad ticker"}))
                    return
                frm = _clean_date((q.get("from") or [today])[0], today)
                till = _clean_date((q.get("till") or [today])[0], today)
                self._send(200, "application/json; charset=utf-8",
                           json.dumps(fetch_ticker(t, frm, till)))
            elif u.path == "/api/series":
                code = (q.get("code") or [""])[0]
                src = (q.get("src") or [""])[0]
                if code not in INSTRUMENTS or src not in ("flow", "conc"):
                    self._send(400, "application/json",
                               json.dumps({"source": "error", "message": "bad params"}))
                    return
                frm = _clean_date((q.get("from") or [today])[0], today)
                till = _clean_date((q.get("till") or [today])[0], today)
                self._send(200, "application/json; charset=utf-8",
                           json.dumps(get_series(src, code, frm, till)))
            else:
                self._send(404, "text/plain; charset=utf-8", "not found")
        except Exception as e:
            self._send(500, "application/json", json.dumps({"source": "error", "message": str(e)}))


HTML = """<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Скринер ОИ · LIVE · валютные фьючерсы</title>
<style>
  :root{
    --bg:#06090d; --panel:#0c121a; --panel2:#0a0e14; --border:#19212c; --border2:#222c3a;
    --text:#d6dee8; --muted:#6b7888; --muted2:#4a5666;
    --green:#2ad17e; --green2:#1b8f55; --red:#f05462; --accent:#4b85ff; --amber:#f5b23a;
    --glass:rgba(255,255,255,.03);
  }
  *{box-sizing:border-box; margin:0; padding:0}
  html,body{height:100%}
  body{background:radial-gradient(1200px 600px at 72% -12%, #0e1722, var(--bg)) fixed; color:var(--text);
    font:13px/1.4 "Segoe UI",system-ui,Arial,sans-serif; overflow:hidden}
  .num{font-variant-numeric:tabular-nums}
  .up{color:var(--green)} .down{color:var(--red)}

  .toolbar{display:flex; align-items:center; gap:13px; height:54px; padding:0 15px;
    background:linear-gradient(180deg,#0c131c,#080c12); border-bottom:1px solid var(--border);
    box-shadow:0 2px 14px rgba(0,0,0,.4); position:relative; z-index:10; overflow-x:auto; overflow-y:hidden}
  .brand{display:flex; flex-direction:column; line-height:1.12; margin-right:2px; white-space:nowrap}
  .brand b{font-size:15px; letter-spacing:.3px}
  .brand small{color:var(--muted); font-size:10.5px}
  .grp{display:flex; align-items:center; gap:7px; white-space:nowrap}
  .grp .lbl{color:var(--muted2); font-size:10px; text-transform:uppercase; letter-spacing:.7px}
  .seg{display:flex; background:#0a0f16; border:1px solid var(--border); border-radius:9px; padding:2px; gap:2px}
  .seg button{border:0; background:transparent; color:var(--muted); font:inherit; font-size:12px;
    padding:5px 10px; border-radius:7px; cursor:pointer; white-space:nowrap; transition:.12s}
  .seg button:hover{color:var(--text)}
  .seg button.on{background:linear-gradient(180deg,#23344e,#1a2740); color:#cfe0ff;
    box-shadow:inset 0 0 0 1px rgba(90,140,255,.28)}
  .stepper{display:flex; align-items:center; background:#0a0f16; border:1px solid var(--border);
    border-radius:9px; overflow:hidden}
  .stepper button{border:0; background:transparent; color:var(--text); font:inherit; font-size:16px;
    width:28px; height:30px; cursor:pointer; transition:.12s}
  .stepper button:hover{background:var(--glass)}
  .stepper #wcount{min-width:24px; text-align:center; font-weight:700; font-size:13px}
  .iconbtn{border:1px solid var(--border); background:#0a0f16; color:var(--muted);
    width:34px; height:32px; border-radius:9px; cursor:pointer; font-size:15px; transition:.12s}
  .iconbtn:hover{color:var(--text)}
  .iconbtn.on{color:var(--green); border-color:var(--green2)}
  .spacer{flex:1; min-width:6px}
  .pill{display:flex; align-items:center; gap:7px; background:#0a0f16; border:1px solid var(--border);
    border-radius:20px; padding:5px 12px; font-size:11.5px; color:var(--muted); white-space:nowrap}
  .dot{width:8px; height:8px; border-radius:50%; background:var(--muted2)}
  .dot.live{background:var(--green); animation:pulse 1.8s infinite}
  .dot.delayed{background:var(--amber)} .dot.err{background:var(--red)}
  @keyframes pulse{0%{box-shadow:0 0 0 0 rgba(42,209,126,.5)}70%{box-shadow:0 0 0 7px rgba(42,209,126,0)}100%{box-shadow:0 0 0 0 rgba(42,209,126,0)}}

  .grid{height:calc(100% - 54px); padding:12px; display:grid; gap:11px;
    grid-template-columns:repeat(var(--cols,2),minmax(0,1fr)); grid-auto-rows:minmax(150px,1fr); overflow:auto}
  .win{position:relative; background:linear-gradient(180deg,#0c121a,#0a0e14); border:1px solid var(--border);
    border-radius:13px; display:flex; flex-direction:column; min-height:150px; min-width:0; overflow:hidden;
    box-shadow:0 6px 20px rgba(0,0,0,.3)}
  .whead{display:flex; align-items:center; flex-wrap:wrap; gap:6px; padding:7px 9px; border-bottom:1px solid var(--border)}
  .wsel{background:#0a0f16; color:var(--text); border:1px solid var(--border2); border-radius:8px;
    font:inherit; font-size:11.5px; padding:4px 6px; cursor:pointer}
  .wsel:focus{outline:0; border-color:var(--accent)}
  .wInstr{max-width:120px; font-weight:600}
  .wPer{max-width:98px}
  .wSrc{max-width:124px} .wMetric{max-width:134px}
  .wDrag{cursor:grab; color:var(--muted2); font-size:15px; line-height:1; padding:0 2px; user-select:none;
    align-self:center; letter-spacing:-2px}
  .wDrag:hover{color:var(--text)} .wDrag:active{cursor:grabbing}
  .win.dragging{opacity:.4}
  .win.drop-before{box-shadow:inset 3px 0 0 0 var(--accent)}
  .win.drop-after{box-shadow:inset -3px 0 0 0 var(--accent)}
  .wGrp{padding:2px}
  .wGrp button{padding:4px 7px; font-size:11px}
  .wSide{border:1px solid var(--border2); background:#0a0f16; border-radius:8px; font:inherit;
    font-size:11.5px; font-weight:700; padding:4px 9px; cursor:pointer; transition:.12s}
  .wSide.buy{color:var(--green); border-color:var(--green2)}
  .wSide.sell{color:var(--red); border-color:#8f2b33}
  .wType{border:1px solid var(--border2); background:#0a0f16; color:var(--muted); border-radius:8px;
    font:inherit; font-size:11.5px; padding:4px 8px; cursor:pointer; white-space:nowrap}
  .wType:hover{color:var(--text)}
  .wnav{display:flex; gap:2px; align-items:center}
  .navbtn{border:1px solid var(--border2); background:#0a0f16; color:var(--muted); border-radius:7px;
    width:25px; height:26px; cursor:pointer; font-size:13px; line-height:1; transition:.12s}
  .navbtn:hover{color:var(--text); background:var(--glass)}
  .wstat{display:flex; align-items:center; gap:6px; margin-left:auto; font-size:12px; white-space:nowrap}
  .dotc{width:8px; height:8px; border-radius:50%}
  .dotc.buy{background:var(--green)} .dotc.sell{background:var(--red)}
  .wchg{font-size:11px; margin-left:2px}
  .wbtn{border:0; background:transparent; color:var(--muted2); cursor:pointer; font-size:13px;
    padding:2px 5px; border-radius:6px; line-height:1}
  .wbtn:hover{color:var(--text); background:var(--glass)}
  .canvas-box{flex:1; position:relative; min-height:0}
  .canvas-box canvas{position:absolute; inset:0; width:100%; height:100%; cursor:crosshair; touch-action:none}
  .tip{position:absolute; pointer-events:none; background:rgba(13,20,30,.96); border:1px solid #2a3648;
    border-radius:9px; padding:6px 9px; font-size:11.5px; white-space:nowrap; opacity:0; transition:opacity .08s;
    box-shadow:0 8px 22px rgba(0,0,0,.5); z-index:5; line-height:1.5}
  .overlay{position:absolute; inset:0; display:flex; align-items:center; justify-content:center;
    color:var(--muted); font-size:12px; text-align:center; padding:14px}
  .overlay.hide{display:none}
  .acc{position:absolute; left:50%; top:6px; transform:translateX(-50%); z-index:6;
    max-width:calc(100% - 14px); text-align:center; background:rgba(22,15,7,.95);
    border:1px solid #d98a2b; border-radius:9px; padding:4px 9px; font-size:11px;
    line-height:1.45; color:#ffd9a8; box-shadow:0 6px 18px rgba(0,0,0,.5)}
  .acc.hide{display:none}
  .acc.live{border-color:#f0a23a; animation:accpulse 1.5s ease-in-out infinite}
  .acc.past{opacity:.85; border-color:#6e5128; background:rgba(16,13,9,.92); color:#d8c2a0}
  .acc .ttl{font-weight:700}
  .acc b{color:#ffb347}
  .acc .q{margin-top:3px; display:flex; gap:6px; align-items:center; justify-content:center; color:#cdd6e0}
  .acc .q button{font:inherit; cursor:pointer; padding:1px 10px; border-radius:7px;
    border:1px solid #2f3b4c; background:#16202c; color:#dfe7f0}
  .acc .q button[data-act=yes]{border-color:#2a7d52; color:#7ee6ad}
  .acc .q button[data-act=no]{border-color:#7d2f38; color:#f0a0a8}
  .acc .q button:hover{filter:brightness(1.25)}
  .acc .q a{color:#8fa1b3; font-size:10px; text-decoration:none; margin-left:4px}
  @keyframes accpulse{0%,100%{box-shadow:0 0 0 0 rgba(240,162,58,0)}50%{box-shadow:0 0 0 5px rgba(240,162,58,.15)}}
  .spin{width:22px; height:22px; border:3px solid var(--border); border-top-color:var(--accent);
    border-radius:50%; animation:s .8s linear infinite}
  @keyframes s{to{transform:rotate(360deg)}}
  ::-webkit-scrollbar{width:9px; height:9px}
  ::-webkit-scrollbar-thumb{background:#1c2735; border-radius:5px}
</style>
</head>
<body>

<div class="toolbar">
  <div class="brand"><b>Скринер ОИ</b><small>валютные фьючерсы · MOEX</small></div>
  <div class="grp"><span class="lbl">окон</span>
    <div class="stepper"><button id="wMinus" title="Убрать окно">−</button>
      <span id="wcount" class="num">2</span>
      <button id="wPlus" title="Добавить окно">+</button></div></div>
  <button id="refresh" class="iconbtn" title="Обновить">⟳</button>
  <button id="autoBtn" class="iconbtn" title="Автообновление каждые 45 сек">⏱</button>
  <div class="spacer"></div>
  <span class="pill"><span class="dot" id="srcDot"></span><span id="srcTxt">загрузка…</span></span>
</div>

<div class="grid" id="grid"></div>

<script>
const INSTR = {
  CR:["Юань","CNY/RUB"], Si:["Доллар","USD/RUB"], Eu:["Евро","EUR/RUB"],
  ED:["Евро/Доллар","EUR/USD"], CNYRUBF:["Юань ∞","вечный"],
  USDRUBF:["Доллар ∞","вечный"], EURRUBF:["Евро ∞","вечный"],
  SS:["Самолёт","SMLT"], NR:["NGM","NGM"],
};
const ORDER = ["CR","Si","Eu","ED","CNYRUBF","USDRUBF","EURRUBF","SS","NR"];
const PERIODS = [["Сегодня",0],["3 дня",2],["Неделя",6],["Месяц",29]];
const TFS = [["5м",5],["15м",15],["30м",30],["1ч",60],["1д",1440]];
const TF_VALS = [5,15,30,60,1440];
const PER_VALS = [0,2,6,29];
const LS_KEY = "oi_screener_v5";
const COL_LONG = "#2ad17e", COL_SHORT = "#f05462";
const COL_OI = "#4b85ff", COL_CONC = "#f5b23a";          // поток ОИ / концентрация HHI
const SRC_VALS = ["futoi","flow","conc"];
const SOURCES = [["ОИ Физ/Юр","futoi"],["Поток сделок","flow"],["Концентрация","conc"]];
const FLOW_METRICS = [["ОИ контракта","oi"],["Дисбаланс ▲▼","disb"]];
const CONC_METRICS = [["HHI агрессоры","hhi_agressive"],["HHI покупки","hhi_buy"],
  ["HHI продажи","hhi_sell"],["HHI пассив","hhi_passive"],["HHI объём","hhi_volume"]];
const MINSPAN = 6;

// одно окно: инструмент, Юр/Физ, сторона (long/short), таймфрейм, период (дней назад), тип (line/candle),
// view = зум/прокрутка: span — сколько ячеек видно (null = весь период), end — правый край, follow — держать последнее
function normMetric(src, m){
  if(src==="flow") return FLOW_METRICS.some(x=>x[1]===m)?m:"oi";
  if(src==="conc") return CONC_METRICS.some(x=>x[1]===m)?m:"hhi_agressive";
  return null; }
function normWin(w){ w=w||{}; const src = SRC_VALS.includes(w.src)?w.src:"futoi"; return {
  code: ORDER.includes(w.code)?w.code:"CR",
  group: w.group==="FIZ"?"FIZ":"YUR",
  side: w.side==="short"?"short":"long",
  tf: TF_VALS.includes(w.tf)?w.tf:5,
  period: PER_VALS.includes(w.period)?w.period:0,
  type: w.type==="candle"?"candle":"line",
  src, metric: normMetric(src, w.metric),
  view: { span:(w.span>0?Math.round(w.span):null), end:null, follow:true } }; }
function defWins(){ return [ normWin({code:"CR",group:"YUR",side:"long"}),
                            normWin({code:"CR",group:"YUR",side:"short"}) ]; }

function loadState(){
  let s=null; try{ s=JSON.parse(localStorage.getItem(LS_KEY)); }catch(e){}
  const auto = s ? s.auto!==false : true;
  const wins = (s && Array.isArray(s.wins) && s.wins.length) ? s.wins.slice(0,8).map(normWin) : defWins();
  return { auto, wins };
}
const state = loadState();
state.store = {}; state.series = {}; state.timer = null; state.ms = 45000; state.hasToken = true; state.loaded = false; state.loadedN = -1;
function saveState(){ try{ localStorage.setItem(LS_KEY, JSON.stringify({
  auto:state.auto,
  wins: state.wins.map(w=>({code:w.code, group:w.group, side:w.side, tf:w.tf, period:w.period, type:w.type, src:w.src, metric:w.metric, span:w.view.span}))
})); }catch(e){} }
function maxPeriodN(){ return state.wins.reduce((m,w)=>Math.max(m,w.period), 0); }

const $ = s => document.querySelector(s);
const fmt = v => Math.round(v).toLocaleString("ru-RU");
const fmtSign = v => (v>0?"+":"") + fmt(v);
const pad = n => String(n).padStart(2,"0");
const todayISO = ()=>{ const d=new Date(); return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`; };
const dmy = s => { const a=s.split("-"); return a[2]+"."+a[1]; };

// ── разбор ответа сервера ──────────────────────────────────────────────────────
function parseRows(p){
  const c = p.columns||[];
  const i = { d:c.indexOf("tradedate"), t:c.indexOf("tradetime"), g:c.indexOf("clgroup"),
              l:c.indexOf("pos_long"), s:c.indexOf("pos_short"), n:c.indexOf("pos"),
              ln:c.indexOf("pos_long_num"), sn:c.indexOf("pos_short_num") };
  const out = { YUR:[], FIZ:[] };
  for(const r of (p.data||[])){
    const g = r[i.g]; if(g!=="YUR" && g!=="FIZ") continue;
    const date=String(r[i.d]), time=String(r[i.t]);
    out[g].push({ date, time, ms:Date.parse(date+"T"+time),
      long:+r[i.l], short:Math.abs(+r[i.s]), net:+r[i.n], lnum:+r[i.ln], snum:+r[i.sn] });
  }
  out.YUR.sort((a,b)=>a.ms-b.ms); out.FIZ.sort((a,b)=>a.ms-b.ms);
  return { byGroup:out, source:p.source||"error", lag:p.lag };
}

// ресэмпл в свечи OHLC по таймфрейму для выбранной стороны (ОИ — это уровень).
// Линия рисуется по close. long/short/lnum/snum берём из последней точки бакета.
function resampleOHLC(rows, tfMin, key){
  if(!rows.length) return [];
  const ms = Math.max(5,tfMin)*60000, out=[]; let cur=null, ck=null;
  for(const r of rows){ const k=Math.floor(r.ms/ms), v=+r[key];
    if(cur===null || k!==ck){ if(cur) out.push(cur); ck=k;
      cur={ ms:r.ms, date:r.date, time:r.time, o:v, h:v, l:v, c:v,
            long:r.long, short:r.short, lnum:r.lnum, snum:r.snum }; }
    else { cur.c=v; if(v>cur.h)cur.h=v; if(v<cur.l)cur.l=v;
      cur.ms=r.ms; cur.date=r.date; cur.time=r.time;
      cur.long=r.long; cur.short=r.short; cur.lnum=r.lnum; cur.snum=r.snum; } }
  if(cur) out.push(cur);
  return out;
}

// ── детектор «крупного участника» (см. scan1.py) ─────────────────────────────────
// На последнем дне ищем набор ОИ от впадины к пику. Если прирост крупный И в основном
// за счёт укрупнения немногих лиц (а не притока новых) — подозрение на одного игрока.
// excess = ΔОИ − (ОИ/лиц во впадине)·Δлиц ;  conc = excess/ΔОИ (доля концентрации).
// Порог набора авто-масштабируется под инструмент (5% медианы ОИ стороны).
function detectAccum(cells, side){
  if(!cells || cells.length < 4) return null;
  const oiKey = side==="long"?"long":"short", nKey = side==="long"?"lnum":"snum";
  const lastDate = cells[cells.length-1].date;
  const day = cells.filter(c=> c.date===lastDate && c[oiKey]>0 && c[nKey]>0);
  if(day.length < 4) return null;
  const ois = cells.map(c=>c[oiKey]).filter(x=>x>0).sort((a,b)=>a-b);
  if(!ois.length) return null;
  const riseMin = Math.max(40000, Math.round(0.05*ois[ois.length>>1]));  // масштаб под инструмент
  let trOI=day[0][oiKey], trN=Math.max(1,day[0][nKey]), peak=null;
  for(const c of day){
    const oi=c[oiKey], nn=Math.max(1,c[nKey]);
    if(oi<trOI){ trOI=oi; trN=nn; }
    const rise=oi-trOI, excess=rise-(trOI/trN)*(nn-trN);
    if(peak===null || rise>peak.rise) peak={rise,excess,oi,nn,time:c.time};
  }
  if(!peak || peak.rise<riseMin || peak.excess < 0.5*peak.rise) return null;
  const lc=day[day.length-1], curOI=lc[oiKey], curN=Math.max(1,lc[nKey]);
  const curRise=curOI-trOI, curExcess=curRise-(trOI/trN)*(curN-trN);
  const active = curRise>=riseMin && curExcess>=0.5*curRise;  // всё ещё набирает сейчас
  return { date:lastDate, side, rise:peak.rise, excess:Math.max(0,Math.round(peak.excess)),
           conc:peak.excess/peak.rise, oi:peak.oi, n:peak.nn, time:peak.time, active };
}

// метки трейдера «реальный участник?» — отдельный ключ localStorage
const ACC_KEY = "oi_accum_labels_v1";
function accLabels(){ try{ return JSON.parse(localStorage.getItem(ACC_KEY)||"{}"); }catch(_){ return {}; } }
function setAccLabel(k,v){ const m=accLabels(); if(v===null) delete m[k]; else m[k]=v;
  try{ localStorage.setItem(ACC_KEY, JSON.stringify(m)); }catch(_){} }

// чип-подсказка + вопрос трейдеру (не зависит от индекса окна)
function renderAccumChip(box, w, ev){
  if(!box) return;
  if(!ev){ box.className="acc hide"; box.innerHTML=""; return; }
  const k=[w.code,w.group,w.side,ev.date].join("|"), lab=accLabels()[k];
  const nm = w.side==="long"?"в ЛОНГ":"в ШОРТ";
  const head = ev.active ? "⚑ Возможно набирается участник" : "⚑ Был набор сегодня";
  let html = `<div class="ttl">${head} ${nm}</div>`
    + `<div>≈ <b>${fmt(ev.excess)}</b> контр. немногими · конц. ${Math.round(ev.conc*100)}% · пик ${ev.time.slice(0,5)}</div>`;
  if(lab==="yes")      html += `<div class="q" style="color:#2ad17e">✓ отмечен как реальный <a href="#" data-act="clear">сброс</a></div>`;
  else if(lab==="no")  html += `<div class="q" style="color:#f05462">✗ отмечен как ложный <a href="#" data-act="clear">сброс</a></div>`;
  else                 html += `<div class="q">Реальный участник? <button data-act="yes">Да</button><button data-act="no">Нет</button></div>`;
  box.className = "acc " + (ev.active?"live":"past");
  box.innerHTML = html;
  box.querySelectorAll("[data-act]").forEach(b=> b.onclick=(e)=>{ e.preventDefault();
    setAccLabel(k, b.dataset.act==="clear"?null:b.dataset.act); renderAccumChip(box, w, ev); });
}

// ── загрузка ────────────────────────────────────────────────────────────────────
// дата N дней назад в формате YYYY-MM-DD (строки сравниваются лексикографически)
function isoBack(n){ const d=new Date(); d.setDate(d.getDate()-n);
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`; }
function rangeFor(){ const n=maxPeriodN(); return { frm: isoBack(n), till: todayISO(), n }; }
// какие ряды AlgoPack нужны (по уникальным code|src среди окон «поток/концентрация»)
function neededSeries(){ const m=new Map();
  for(const w of state.wins){ if(w.src!=="futoi") m.set(w.code+"|"+w.src, {code:w.code, src:w.src}); }
  return [...m.values()]; }
async function loadAll(){
  const {frm,till,n} = rangeFor();
  const needs = neededSeries();
  try{
    const reqs = [ fetch(`/api/all?from=${frm}&till=${till}`, {cache:"no-store"}).then(r=>r.json()) ];
    for(const k of needs) reqs.push(
      fetch(`/api/series?src=${k.src}&code=${k.code}&from=${frm}&till=${till}`, {cache:"no-store"})
        .then(r=>r.json()).then(j=>["S", k.code+"|"+k.src, j])
        .catch(_=>["S", k.code+"|"+k.src, {source:"error", message:"сбой сети"}]) );
    const res = await Promise.all(reqs);
    const j0 = res[0];
    for(const t of ORDER) state.store[t] = parseRows(j0.data[t]||{});
    state.hasToken = j0.token;
    for(let r=1;r<res.length;r++) state.series[res[r][1]] = res[r][2];
    state.loaded = true; state.loadedN = n;
    drawAll(); updateStatus();
  }catch(e){
    $("#srcTxt").textContent = "сервер недоступен"; $("#srcDot").className = "dot err";
  }
}
// точечная догрузка одного ряда (смена инструмента/источника в окне — без полной перезагрузки)
async function loadOneSeries(code, src){
  const {frm,till} = rangeFor(), key=code+"|"+src;
  try{ const j = await (await fetch(`/api/series?src=${src}&code=${code}&from=${frm}&till=${till}`,
        {cache:"no-store"})).json(); state.series[key]=j; }
  catch(_){ state.series[key]={source:"error", message:"сбой сети"}; }
  for(let i=0;i<state.wins.length;i++){ const w=state.wins[i];
    if(w.src===src && w.code===code) drawWindow(i); }
}
// смена периода окна: дозагружаем с сервера только если нужен диапазон больше уже загруженного
function setWinPeriod(i, val){ const w=state.wins[i]; w.period=val; w.view={span:null,end:null,follow:true};
  if(val>state.loadedN) loadAll(); else { drawWindow(i); } saveState(); }
function updateStatus(){
  const vals = ORDER.map(t=>state.store[t]).filter(Boolean);
  const live = vals.find(v=>v.source==="live"), delayed = vals.find(v=>v.source==="delayed");
  const dot=$("#srcDot"), txt=$("#srcTxt");
  const now=new Date().toLocaleTimeString("ru-RU",{hour:"2-digit",minute:"2-digit",second:"2-digit"});
  if(live){ dot.className="dot live"; txt.textContent=`LIVE · ${now}`; }
  else if(delayed){ dot.className="dot delayed"; txt.textContent=`ЗАДЕРЖКА ${delayed.lag} дн · обнови токен`; }
  else { dot.className="dot err"; txt.textContent = state.hasToken? "нет данных от MOEX" : "нет токена"; }
}

// ── сетка окон ────────────────────────────────────────────────────────────────────
const RG = [];   // на окно: {series,n,geo,vmin,vmax,intraday,hoverIdx}
function colsFor(n){ return n<=1?1 : n<=2?n : n<=4?2 : n<=6?3 : 4; }

function makeCard(){
  const card=document.createElement("div"); card.className="win";
  card.innerHTML =
    '<div class="whead">'
    + '<span class="wDrag" title="Перетащить — поменять окна местами" draggable="true">≡</span>'
    + '<select class="wsel wInstr"></select>'
    + '<select class="wsel wSrc" title="Источник данных"></select>'
    + '<div class="seg wGrp"><button data-g="YUR">Юр</button><button data-g="FIZ">Физ</button></div>'
    + '<button class="wSide" title="Покупки / Продажи"></button>'
    + '<select class="wsel wMetric" title="Показатель"></select>'
    + '<select class="wsel wTf" title="Таймфрейм"></select>'
    + '<select class="wsel wPer" title="Период графика"></select>'
    + '<button class="wType" title="Линия / Свечи"></button>'
    + '<div class="wnav">'
    +   '<button class="navbtn nOut" title="Отдалить">−</button>'
    +   '<button class="navbtn nIn" title="Приблизить">+</button>'
    +   '<button class="navbtn nLeft" title="Листать назад">‹</button>'
    +   '<button class="navbtn nRight" title="Листать вперёд">›</button>'
    +   '<button class="navbtn nFit" title="Весь период">⤢</button>'
    + '</div>'
    + '<div class="wstat"><span class="dotc buy"></span><span class="num wval">—</span>'
    + '<span class="num wchg"></span></div>'
    + '<button class="wbtn wDel" title="Убрать окно">✕</button>'
    + '</div>'
    + '<div class="canvas-box"><canvas></canvas><div class="tip"></div>'
    + '<div class="acc hide"></div>'
    + '<div class="overlay"><div class="spin"></div></div></div>';
  const sel=card.querySelector(".wInstr");
  for(const code of ORDER){ const o=document.createElement("option");
    o.value=code; o.textContent=INSTR[code][0]; sel.appendChild(o); }
  const srcSel=card.querySelector(".wSrc");
  for(const [lab,val] of SOURCES){ const o=document.createElement("option");
    o.value=val; o.textContent=lab; srcSel.appendChild(o); }
  const mSel=card.querySelector(".wMetric");
  const tfs=card.querySelector(".wTf");
  for(const [lab,val] of TFS){ const o=document.createElement("option");
    o.value=String(val); o.textContent=lab; tfs.appendChild(o); }
  const pers=card.querySelector(".wPer");
  for(const [lab,val] of PERIODS){ const o=document.createElement("option");
    o.value=String(val); o.textContent=lab; pers.appendChild(o); }
  const idx = ()=>[...$("#grid").children].indexOf(card);
  // при смене инструмента/источника на «поток/концентрацию» нужен ряд по новому контракту
  const ensureSeries = (i)=>{ const w=state.wins[i];
    if(w.src!=="futoi" && !state.series[w.code+"|"+w.src]){ drawWindow(i); loadOneSeries(w.code, w.src); }
    else drawWindow(i); };
  sel.onchange = ()=>{ const i=idx(), w=state.wins[i]; w.code=sel.value;
    w.view={span:null,end:null,follow:true}; ensureSeries(i); saveState(); };
  srcSel.onchange = ()=>{ const i=idx(), w=state.wins[i]; w.src=srcSel.value;
    w.metric=normMetric(w.src, w.metric);
    if(w.src==="conc" && w.period<2){ w.period=6; delete state.series[w.code+"|conc"]; }  // HHI дневной → период ≥ дней
    w.view={span:null,end:null,follow:true};
    syncCard(card,i); ensureSeries(i); saveState(); };
  mSel.onchange = ()=>{ const i=idx(), w=state.wins[i]; w.metric=mSel.value;
    w.view={span:null,end:null,follow:true}; syncCard(card,i); drawWindow(i); saveState(); };
  card.querySelectorAll(".wGrp button").forEach(b=> b.onclick=()=>{
    const i=idx(); state.wins[i].group=b.dataset.g; syncCard(card,i); drawWindow(i); saveState(); });
  card.querySelector(".wSide").onclick = ()=>{ const i=idx(), w=state.wins[i];
    w.side = w.side==="long"?"short":"long"; syncCard(card,i); drawWindow(i); saveState(); };
  tfs.onchange = ()=>{ const i=idx(), w=state.wins[i]; w.tf=+tfs.value;
    w.view={span:null,end:null,follow:true}; drawWindow(i); saveState(); };
  pers.onchange = ()=>{ setWinPeriod(idx(), +pers.value); };
  card.querySelector(".wType").onclick = ()=>{ const i=idx(), w=state.wins[i];
    w.type = w.type==="candle"?"line":"candle"; syncCard(card,i); drawWindow(i); saveState(); };
  card.querySelector(".nOut").onclick  = ()=>{ const i=idx(); zoomBy(i,1.5,1);  drawWindow(i); saveState(); };
  card.querySelector(".nIn").onclick   = ()=>{ const i=idx(); zoomBy(i,0.66,1); drawWindow(i); saveState(); };
  card.querySelector(".nLeft").onclick = ()=> panBtn(idx(),-1);
  card.querySelector(".nRight").onclick= ()=> panBtn(idx(), 1);
  card.querySelector(".nFit").onclick  = ()=>{ const i=idx(); fitView(i); drawWindow(i); saveState(); };
  card.querySelector(".wDel").onclick  = ()=> removeWin(idx());

  // перетаскивание окон местами (ручка ≡)
  const handle=card.querySelector(".wDrag");
  handle.addEventListener("dragstart", e=>{ dragFrom=idx(); card.classList.add("dragging");
    e.dataTransfer.effectAllowed="move"; try{ e.dataTransfer.setData("text/plain",String(dragFrom));
      e.dataTransfer.setDragImage(card, 30, 18); }catch(_){} });
  handle.addEventListener("dragend", ()=>{ card.classList.remove("dragging"); clearDropMark(); dragFrom=-1; });
  card.addEventListener("dragover", e=>{ if(dragFrom<0) return; e.preventDefault(); e.dataTransfer.dropEffect="move";
    const r=card.getBoundingClientRect(); markDrop(card, (e.clientX-r.left)>r.width/2); });
  card.addEventListener("drop", e=>{ if(dragFrom<0) return; e.preventDefault();
    const r=card.getBoundingClientRect(); moveWin(dragFrom, idx(), (e.clientX-r.left)>r.width/2); clearDropMark(); });

  const cv=card.querySelector("canvas"); let drag=null, touch=null;
  cv.addEventListener("wheel", ev=>{ ev.preventDefault(); const i=idx(), g=RG[i]; if(!g||!g.total) return;
    let frac=1; if(g.geo){ const r=cv.getBoundingClientRect();
      frac=Math.max(0,Math.min(1,(ev.clientX-r.left-g.geo.padL)/g.geo.plotW)); }
    zoomBy(i, ev.deltaY>0?1.25:0.8, frac); drawWindow(i); saveState(); }, {passive:false});
  cv.addEventListener("mousedown", ev=>{ const i=idx(), g=RG[i]; if(!g||!g.total) return; const w=state.wins[i];
    const span=w.view.span==null?g.total:Math.min(w.view.span,g.total);
    drag={ span, end:(w.view.follow?g.total-1:(w.view.end==null?g.total-1:w.view.end)),
      x:ev.clientX, step:(g.geo?g.geo.plotW:1)/Math.max(1,span-1) }; cv.style.cursor="grabbing"; });
  cv.addEventListener("mousemove", ev=>{ const i=idx(), g=RG[i]; if(!g||!g.total) return;
    if(drag){ applyPan(i, drag, Math.round((ev.clientX-drag.x)/drag.step)); RG[i].hoverIdx=null; drawWindow(i); return; }
    if(!g.geo||!g.n) return; const r=cv.getBoundingClientRect(), mx=ev.clientX-r.left;
    g.hoverIdx = g.n<=1?0 : Math.max(0,Math.min(g.n-1, Math.round((mx-g.geo.padL)/g.geo.plotW*(g.n-1))));
    drawWindow(i); });
  const stopDrag=()=>{ if(drag){ drag=null; cv.style.cursor="crosshair"; saveState(); } };
  cv.addEventListener("mouseup", stopDrag);
  cv.addEventListener("mouseleave", ()=>{ const i=idx(); stopDrag();
    if(RG[i]) RG[i].hoverIdx=null; card.querySelector(".tip").style.opacity=0; drawWindow(i); });

  cv.addEventListener("touchstart", ev=>{ const i=idx(), g=RG[i]; if(!g||!g.total) return; const w=state.wins[i];
    const span=w.view.span==null?g.total:Math.min(w.view.span,g.total),
      end=(w.view.follow?g.total-1:(w.view.end==null?g.total-1:w.view.end));
    if(ev.touches.length===1) touch={mode:"pan", x:ev.touches[0].clientX, span, end, step:(g.geo?g.geo.plotW:1)/Math.max(1,span-1)};
    else touch={mode:"pinch", d0:Math.abs(ev.touches[0].clientX-ev.touches[1].clientX)||1, span, end}; }, {passive:true});
  cv.addEventListener("touchmove", ev=>{ const i=idx(), g=RG[i]; if(!g||!touch||!g.total) return; ev.preventDefault(); const w=state.wins[i];
    if(touch.mode==="pan" && ev.touches.length===1){ applyPan(i, touch, Math.round((ev.touches[0].clientX-touch.x)/touch.step)); drawWindow(i); }
    else if(touch.mode==="pinch" && ev.touches.length>=2){ const d=Math.abs(ev.touches[0].clientX-ev.touches[1].clientX)||1;
      let ns=Math.max(MINSPAN, Math.min(g.total, Math.round(touch.span*(touch.d0/d))));
      if(ns>=g.total) w.view={span:null,end:null,follow:true};
      else { const e=Math.min(g.total-1, Math.max(ns-1, touch.end)); w.view={span:ns, end:(e>=g.total-1?null:e), follow:(e>=g.total-1)}; }
      drawWindow(i); } }, {passive:false});
  cv.addEventListener("touchend", ()=>{ if(touch){ touch=null; saveState(); } }, {passive:true});
  return card;
}

function fillMetric(card, w){
  const sel=card.querySelector(".wMetric");
  const list = w.src==="flow"?FLOW_METRICS : w.src==="conc"?CONC_METRICS : [];
  sel.innerHTML=""; for(const [lab,val] of list){ const o=document.createElement("option");
    o.value=val; o.textContent=lab; sel.appendChild(o); }
  if(list.length) sel.value=w.metric;
}
function syncCard(card,i){
  const w=state.wins[i];
  card.querySelector(".wInstr").value=w.code;
  card.querySelector(".wSrc").value=w.src;
  fillMetric(card, w);
  const isFutoi = w.src==="futoi";
  const showType = isFutoi || (w.src==="flow" && w.metric==="oi");
  const disp=(el,on)=>{ if(el) el.style.display = on?"":"none"; };
  disp(card.querySelector(".wGrp"),    isFutoi);
  disp(card.querySelector(".wSide"),   isFutoi);
  disp(card.querySelector(".wMetric"), !isFutoi);
  disp(card.querySelector(".wType"),   showType);
  disp(card.querySelector(".wTf"),     w.src!=="conc");      // hi2 — дневной, ТФ не нужен
  card.querySelectorAll(".wGrp button").forEach(b=> b.classList.toggle("on", b.dataset.g===w.group));
  const sb=card.querySelector(".wSide"); sb.textContent=w.side==="long"?"Покупки":"Продажи";
  sb.className="wSide "+(w.side==="long"?"buy":"sell");
  card.querySelector(".wTf").value=String(w.tf);
  card.querySelector(".wPer").value=String(w.period);
  card.querySelector(".wType").textContent = w.type==="candle"?"▦ Свечи":"〰 Линия";
  const dotc=card.querySelector(".wstat .dotc");
  if(isFutoi){ dotc.className="dotc "+(w.side==="long"?"buy":"sell"); dotc.style.background=""; }
  else { dotc.className="dotc"; dotc.style.background = w.src==="conc"?COL_CONC : (w.metric==="disb"?"#9aa7b5":COL_OI); }
  card.querySelector(".wDel").style.visibility = state.wins.length>1 ? "visible":"hidden";
}

function buildGrid(){
  const grid=$("#grid");
  grid.style.setProperty("--cols", String(colsFor(state.wins.length)));
  while(grid.children.length > state.wins.length) grid.removeChild(grid.lastChild);
  while(grid.children.length < state.wins.length) grid.appendChild(makeCard());
  RG.length = state.wins.length;
  [...grid.children].forEach((c,i)=> syncCard(c,i));
  $("#wcount").textContent = state.wins.length;
}
function drawAll(){ buildGrid(); for(let i=0;i<state.wins.length;i++) drawWindow(i); }

function nextWin(){ const used=new Set(state.wins.map(w=>w.code));
  return normWin({ code: ORDER.find(c=>!used.has(c))||"CR", group:"YUR", side:"long" }); }
function setCount(n){ n=Math.max(1,Math.min(8,n));
  while(state.wins.length<n) state.wins.push(nextWin());
  while(state.wins.length>n){ state.wins.pop(); RG.pop(); }
  drawAll(); saveState(); }
function removeWin(i){ if(state.wins.length<=1) return; state.wins.splice(i,1); RG.splice(i,1); drawAll(); saveState(); }

// ── перетаскивание окон местами ───────────────────────────────────────────────
let dragFrom=-1;
function clearDropMark(){ [...$("#grid").children].forEach(c=>c.classList.remove("drop-before","drop-after")); }
function markDrop(card, after){ clearDropMark(); card.classList.add(after?"drop-after":"drop-before"); }
function moveWin(from, to, after){
  if(from<0) return;
  let insert = after ? to+1 : to;            // куда вставить (в исходных индексах)
  if(from < insert) insert--;                // удаление сдвигает правую часть влево
  if(insert===from) return;                  // окно осталось на месте
  const w=state.wins.splice(from,1)[0], g=RG.splice(from,1)[0];
  insert = Math.max(0, Math.min(state.wins.length, insert));
  state.wins.splice(insert,0,w); RG.splice(insert,0,g);
  drawAll(); saveState();
}

// ── зум / прокрутка ───────────────────────────────────────────────────────────
function zoomBy(i, factor, frac){
  const g=RG[i]; if(!g||!g.total) return; const w=state.wins[i], n=g.total;
  let span = w.view.span==null ? n : Math.min(w.view.span, n);
  let ns = Math.max(MINSPAN, Math.min(n, Math.round(span*factor)));
  if(ns>=n){ w.view={span:null, end:null, follow:true}; return; }
  const end = w.view.follow ? n-1 : (w.view.end==null ? n-1 : Math.min(n-1, w.view.end));
  const start = Math.max(0, end-span+1);
  if(frac==null) frac=1;
  const anchor = start + Math.round(frac*(span-1));
  let nStart = Math.max(0, Math.min(n-ns, Math.round(anchor - frac*(ns-1))));
  const nEnd = nStart+ns-1;
  w.view={ span:ns, end:(nEnd>=n-1?null:nEnd), follow:(nEnd>=n-1) };
}
function applyPan(i, base, dCells){
  const g=RG[i]; if(!g||!g.total) return; const w=state.wins[i];
  if(base.span>=g.total) return;
  let end = Math.max(base.span-1, Math.min(g.total-1, base.end - dCells));   // тянем вправо → к старым
  w.view={ span:base.span, end:(end>=g.total-1?null:end), follow:(end>=g.total-1) };
}
function panBtn(i, dir){
  const g=RG[i]; if(!g||!g.total) return; const w=state.wins[i];
  const span = w.view.span==null ? g.total : Math.min(w.view.span, g.total); if(span>=g.total) return;
  const end0 = w.view.follow ? g.total-1 : (w.view.end==null ? g.total-1 : w.view.end);
  let end = Math.max(span-1, Math.min(g.total-1, end0 + dir*Math.max(1,Math.round(span*0.3))));
  w.view={ span, end:(end>=g.total-1?null:end), follow:(end>=g.total-1) };
  drawWindow(i); saveState();
}
function fitView(i){ state.wins[i].view={ span:null, end:null, follow:true }; }

function roundRect(ctx,x,y,w,h,r){ ctx.beginPath(); ctx.moveTo(x+r,y);
  ctx.arcTo(x+w,y,x+w,y+h,r); ctx.arcTo(x+w,y+h,x,y+h,r);
  ctx.arcTo(x,y+h,x,y,r); ctx.arcTo(x,y,x+w,y,r); ctx.closePath(); }

// ── рисуем одно окно: одна сторона (Покупки/Продажи), линия или свечи, с зумом/прокруткой ──
function drawWindow(i){
  const card=$("#grid").children[i]; if(!card) return;
  const w=state.wins[i];
  if(w.src!=="futoi"){ drawSeries(i, card, w); return; }   // «поток»/«концентрация» — отдельный рисовальщик
  const st=state.store[w.code];
  const allRows = st? (st.byGroup[w.group]||[]) : [];
  const cutoff = isoBack(w.period);                        // период этого окна (даты — строки, сравнение лексикографически)
  const base = w.period>=state.loadedN ? allRows : allRows.filter(r=> r.date >= cutoff);
  const cells = resampleOHLC(base, w.tf, w.side);          // w.side = "long" | "short"
  RG[i]=RG[i]||{hoverIdx:null}; RG[i].total=cells.length;
  const cv=card.querySelector("canvas"), box=cv.parentElement, dpr=window.devicePixelRatio||1;
  const W=box.clientWidth, H=box.clientHeight; cv.width=W*dpr; cv.height=H*dpr;
  const ctx=cv.getContext("2d"); ctx.setTransform(dpr,0,0,dpr,0,0); ctx.clearRect(0,0,W,H);
  const ov=card.querySelector(".overlay");
  const col = w.side==="long"?COL_LONG:COL_SHORT;

  if(!cells.length){
    ov.classList.remove("hide");
    ov.innerHTML = state.loaded
      ? `<div>${st&&st.source==="error" ? "нет данных — проверь токен" : "нет данных за период"}</div>`
      : '<div class="spin"></div>';
    card.querySelector(".wval").textContent="—"; card.querySelector(".wchg").textContent="";
    RG[i].geo=null; RG[i].n=0; RG[i].accum=null;
    renderAccumChip(card.querySelector(".acc"), w, null); return;
  }
  ov.classList.add("hide");

  // детектор крупного участника (только ФИЗ — крупный физик; по Юр это норма)
  const accEv = w.group==="FIZ" ? detectAccum(cells, w.side) : null;
  RG[i].accum = accEv;
  renderAccumChip(card.querySelector(".acc"), w, accEv);

  // видимый срез (зум/прокрутка)
  const total=cells.length, v=w.view;
  const span = v.span==null ? total : Math.min(v.span, total);
  const end  = v.follow ? total-1 : Math.min(total-1, v.end==null?total-1:v.end);
  const start= Math.max(0, end-span+1);
  const vis = cells.slice(start, end+1);
  const n=vis.length, last=vis[n-1];
  RG[i].series=vis; RG[i].n=n;

  card.querySelector(".wval").textContent=fmt(last.c);
  const chg=last.c - vis[0].o, ce=card.querySelector(".wchg");
  ce.textContent="Δ"+fmtSign(chg); ce.className="num wchg "+(chg>=0?"up":"down");

  const padL=56,padR=14,padT=10,padB=20, plotW=W-padL-padR, plotH=H-padT-padB;
  let vmin=Infinity,vmax=-Infinity;
  for(const c of vis){ if(c.h>vmax)vmax=c.h; if(c.l<vmin)vmin=c.l; }
  if(vmin===vmax){vmin-=1;vmax+=1;} const pv=(vmax-vmin)*0.1; vmin-=pv; vmax+=pv;
  const X=k=> padL+(n<=1?plotW/2:k/(n-1)*plotW);
  const Y=val=> padT+(1-(val-vmin)/(vmax-vmin))*plotH;
  const intraday = vis[0].date===vis[n-1].date;
  RG[i].geo={padL,padR,padT,padB,plotW,plotH,W,H}; RG[i].intraday=intraday;

  ctx.font="10px Segoe UI,Arial"; ctx.textBaseline="middle";
  for(let k=0;k<=4;k++){ const val=vmin+(vmax-vmin)*k/4, y=Y(val);
    ctx.strokeStyle="#121a24"; ctx.beginPath(); ctx.moveTo(padL,y); ctx.lineTo(W-padR,y); ctx.stroke();
    ctx.fillStyle="#5a6776"; ctx.textAlign="right"; ctx.fillText(fmt(val),padL-7,y); }
  ctx.textAlign="center"; ctx.textBaseline="top";
  const xt=Math.min(6,n); let prev="";
  for(let k=0;k<xt;k++){ const ci=Math.round(k/(xt-1||1)*(n-1)), x=X(ci);
    const lab = intraday ? vis[ci].time.slice(0,5) : dmy(vis[ci].date);
    if(lab!==prev||intraday){ ctx.fillStyle="#4d5a69"; ctx.fillText(lab,x,padT+plotH+4); prev=lab; } }

  if(w.type==="candle"){
    const cw=Math.max(1.5, Math.min(14, plotW/Math.max(1,n)*0.66));
    for(let k=0;k<n;k++){ const c=vis[k], x=X(k), up=c.c>=c.o, hex=up?COL_LONG:COL_SHORT;
      ctx.strokeStyle=hex; ctx.fillStyle=hex; ctx.lineWidth=1;
      ctx.beginPath(); ctx.moveTo(x,Y(c.h)); ctx.lineTo(x,Y(c.l)); ctx.stroke();
      const yo=Y(c.o), yc=Y(c.c), top=Math.min(yo,yc), hgt=Math.max(1,Math.abs(yc-yo));
      ctx.fillRect(x-cw/2, top, cw, hgt); }
  } else {
    const grd=ctx.createLinearGradient(0,padT,0,padT+plotH);
    grd.addColorStop(0,col+"2e"); grd.addColorStop(1,col+"00");
    ctx.beginPath(); ctx.moveTo(X(0),padT+plotH);
    for(let k=0;k<n;k++) ctx.lineTo(X(k),Y(vis[k].c));
    ctx.lineTo(X(n-1),padT+plotH); ctx.closePath(); ctx.fillStyle=grd; ctx.fill();
    ctx.beginPath(); ctx.moveTo(X(0),Y(vis[0].c));
    for(let k=1;k<n;k++) ctx.lineTo(X(k),Y(vis[k].c));
    ctx.strokeStyle=col; ctx.lineWidth=1.7; ctx.lineJoin="round"; ctx.stroke();
    if(n===1){ ctx.fillStyle=col; ctx.beginPath(); ctx.arc(X(0),Y(vis[0].c),3,0,7); ctx.fill(); }
  }

  // ярлык последнего значения
  (function(){ const val=last.c, y=Y(val), t=fmt(val); ctx.font="bold 10px Segoe UI,Arial";
    const tw=ctx.measureText(t).width, bx=Math.min(X(n-1), W-padR-tw/2-3);
    ctx.fillStyle=col; roundRect(ctx,bx-tw/2-6,y-8,tw+12,16,4); ctx.fill();
    ctx.fillStyle="#06100a"; ctx.textAlign="center"; ctx.textBaseline="middle"; ctx.fillText(t,bx,y); })();

  const hi=RG[i].hoverIdx, tip=card.querySelector(".tip");
  if(hi!=null && hi>=0 && hi<n){
    const c=vis[hi], x=X(hi), y=Y(c.c);
    ctx.save(); ctx.strokeStyle="#36465a"; ctx.setLineDash([4,4]);
    ctx.beginPath(); ctx.moveTo(x,padT); ctx.lineTo(x,padT+plotH); ctx.stroke(); ctx.setLineDash([]);
    ctx.fillStyle=col; ctx.beginPath(); ctx.arc(x,y,3.5,0,7); ctx.fill();
    ctx.strokeStyle="#fff"; ctx.lineWidth=1.2; ctx.stroke(); ctx.restore();
    const lic = w.side==="long"? c.lnum : c.snum, nm = w.side==="long"?"Покупки":"Продажи";
    tip.style.opacity=1;
    tip.innerHTML = `${intraday?"":dmy(c.date)+" "}${c.time.slice(0,5)}`
      + `<br><span style="color:${col}">${nm} ${fmt(c.c)}</span> · лиц ${lic}`
      + (w.type==="candle" ? `<br>О ${fmt(c.o)} · Макс ${fmt(c.h)} · Мин ${fmt(c.l)} · Закр ${fmt(c.c)}` : ``);
    let tx=x+12; if(tx+tip.offsetWidth>W) tx=x-tip.offsetWidth-12; if(tx<2)tx=2;
    tip.style.left=tx+"px"; tip.style.top=(padT+2)+"px";
  } else { tip.style.opacity=0; }
}

// ресэмпл «потока»: ОИ контракта — уровень (O/H/L/C), объёмы покупок/продаж — сумма за бакет
function resampleFlow(objs, tfMin){
  if(!objs.length) return [];
  const ms=Math.max(5,tfMin)*60000, out=[]; let cur=null, ck=null;
  for(const r of objs){ const k=Math.floor(r.ms/ms);
    if(cur===null||k!==ck){ if(cur)out.push(cur); ck=k;
      cur={ms:r.ms,date:r.date,time:r.time, o:r.oi,h:r.oi,l:r.oi,c:r.oi, vb:r.vb,vs:r.vs, close:r.close}; }
    else { cur.c=r.oi; if(r.oi>cur.h)cur.h=r.oi; if(r.oi<cur.l)cur.l=r.oi;
      cur.vb+=r.vb; cur.vs+=r.vs; cur.close=r.close; cur.ms=r.ms; cur.date=r.date; cur.time=r.time; } }
  if(cur)out.push(cur); return out;
}

// ── рисуем окно «Поток сделок» (tradestats) / «Концентрация» (hi2) ──────────────────
// Делит общий зум/прокрутку/ховер с futoi-окнами (читает те же RG[i].geo/total/series).
function drawSeries(i, card, w){
  const cv=card.querySelector("canvas"), box=cv.parentElement, dpr=window.devicePixelRatio||1;
  const W=box.clientWidth, H=box.clientHeight; cv.width=W*dpr; cv.height=H*dpr;
  const ctx=cv.getContext("2d"); ctx.setTransform(dpr,0,0,dpr,0,0); ctx.clearRect(0,0,W,H);
  const ov=card.querySelector(".overlay");
  RG[i]=RG[i]||{hoverIdx:null}; RG[i].accum=null; renderAccumChip(card.querySelector(".acc"), w, null);
  const fail = msg => { ov.classList.remove("hide");
    ov.innerHTML = state.loaded ? `<div>${msg}</div>` : '<div class="spin"></div>';
    card.querySelector(".wval").textContent="—"; card.querySelector(".wchg").textContent="";
    RG[i].geo=null; RG[i].n=0; RG[i].total=0; RG[i].series=null; };

  const j = state.series[w.code+"|"+w.src];
  if(!j){ fail("загрузка…"); return; }
  if(j.source==="error"){ fail(j.message||"нет данных"); return; }
  const cols=j.columns||[], ci=name=>cols.indexOf(name);

  let cells=[], valOf, mode, col=COL_OI, ttl="", fmtVal=fmt;
  if(w.src==="flow"){
    const iD=ci("date"),iT=ci("time"),iOI=ci("oi"),iVb=ci("vb"),iVs=ci("vs"),iC=ci("close");
    const objs=(j.data||[]).map(r=>({date:r[iD],time:r[iT],ms:Date.parse(r[iD]+"T"+r[iT]),
      oi:+r[iOI]||0, vb:+r[iVb]||0, vs:+r[iVs]||0, close:+r[iC]||0}))
      .filter(o=>o.date>=isoBack(w.period));
    cells=resampleFlow(objs, w.tf);
    if(w.metric==="disb"){ mode="hist"; ttl="Дисбаланс";
      for(const c of cells){ const tot=c.vb+c.vs; c.v = tot? (c.vb-c.vs)/tot*100 : 0; }
      valOf=c=>c.v; fmtVal=v=>(v>0?"+":"")+Math.round(v)+"%"; }
    else { mode=(w.type==="candle"?"candle":"line"); col=COL_OI; ttl="ОИ контракта"; valOf=c=>c.c; }
  } else {  // conc — дневные точки HHI
    const iD=ci("date"),iT=ci("time"),iM=ci(w.metric);
    cells=(j.data||[]).map(r=>({date:r[iD],time:r[iT],ms:Date.parse(r[iD]+"T"+r[iT]),
      c:(iM>=0?+r[iM]:null)})).filter(c=>c.c!=null && c.date>=isoBack(w.period));
    mode="line"; col=COL_CONC; ttl="HHI"; valOf=c=>c.c;
  }
  RG[i].total=cells.length;
  if(!cells.length){ fail(w.src==="conc"
      ? "HHI считается раз в день (≈18:50). Выбери период ≥ «3 дня»"
      : "нет данных за период"); return; }
  ov.classList.add("hide");

  const total=cells.length, v=w.view;
  const span=v.span==null?total:Math.min(v.span,total);
  const end=v.follow?total-1:Math.min(total-1, v.end==null?total-1:v.end);
  const start=Math.max(0,end-span+1);
  const vis=cells.slice(start,end+1), n=vis.length, last=vis[n-1];
  RG[i].series=vis; RG[i].n=n;

  card.querySelector(".wval").textContent = fmtVal(valOf(last));
  const chg=valOf(last)-valOf(vis[0]), ce=card.querySelector(".wchg");
  ce.textContent = "Δ"+(w.metric==="disb" ? (chg>0?"+":"")+Math.round(chg)+"п.п." : fmtSign(chg));
  ce.className="num wchg "+(chg>=0?"up":"down");

  const padL=56,padR=14,padT=10,padB=20, plotW=W-padL-padR, plotH=H-padT-padB;
  let vmin=Infinity,vmax=-Infinity;
  if(mode==="candle"){ for(const c of vis){ if(c.h>vmax)vmax=c.h; if(c.l<vmin)vmin=c.l; } }
  else if(mode==="hist"){ let mx=0; for(const c of vis) mx=Math.max(mx,Math.abs(valOf(c))); mx=Math.max(mx,5); vmin=-mx; vmax=mx; }
  else { for(const c of vis){ const y=valOf(c); if(y>vmax)vmax=y; if(y<vmin)vmin=y; } }
  if(vmin===vmax){vmin-=1;vmax+=1;}
  if(mode!=="hist"){ const pv=(vmax-vmin)*0.1; vmin-=pv; vmax+=pv; }
  const X=k=> padL+(n<=1?plotW/2:k/(n-1)*plotW);
  const Y=val=> padT+(1-(val-vmin)/(vmax-vmin))*plotH;
  const intraday = vis[0].date===vis[n-1].date;
  RG[i].geo={padL,padR,padT,padB,plotW,plotH,W,H}; RG[i].intraday=intraday;

  ctx.font="10px Segoe UI,Arial"; ctx.textBaseline="middle";
  for(let k=0;k<=4;k++){ const val=vmin+(vmax-vmin)*k/4, y=Y(val);
    ctx.strokeStyle="#121a24"; ctx.beginPath(); ctx.moveTo(padL,y); ctx.lineTo(W-padR,y); ctx.stroke();
    ctx.fillStyle="#5a6776"; ctx.textAlign="right";
    ctx.fillText(mode==="hist"? (val>0?"+":"")+Math.round(val)+"%" : fmt(val), padL-7, y); }
  ctx.textAlign="center"; ctx.textBaseline="top";
  const xt=Math.min(6,n); let prev="";
  for(let k=0;k<xt;k++){ const cci=Math.round(k/(xt-1||1)*(n-1)), x=X(cci);
    const lab = intraday ? vis[cci].time.slice(0,5) : dmy(vis[cci].date);
    if(lab!==prev||intraday){ ctx.fillStyle="#4d5a69"; ctx.fillText(lab,x,padT+plotH+4); prev=lab; } }

  if(mode==="hist"){
    const y0=Y(0); ctx.strokeStyle="#2a3648"; ctx.beginPath(); ctx.moveTo(padL,y0); ctx.lineTo(W-padR,y0); ctx.stroke();
    const bw=Math.max(1.5, Math.min(16, plotW/Math.max(1,n)*0.7));
    for(let k=0;k<n;k++){ const val=valOf(vis[k]), x=X(k), y=Y(val);
      ctx.fillStyle = val>=0?COL_LONG:COL_SHORT;
      ctx.fillRect(x-bw/2, Math.min(y,y0), bw, Math.max(1,Math.abs(y-y0))); }
  } else if(mode==="candle"){
    const cw=Math.max(1.5, Math.min(14, plotW/Math.max(1,n)*0.66));
    for(let k=0;k<n;k++){ const c=vis[k], x=X(k), up=c.c>=c.o, hex=up?COL_LONG:COL_SHORT;
      ctx.strokeStyle=hex; ctx.fillStyle=hex; ctx.lineWidth=1;
      ctx.beginPath(); ctx.moveTo(x,Y(c.h)); ctx.lineTo(x,Y(c.l)); ctx.stroke();
      const yo=Y(c.o), yc=Y(c.c); ctx.fillRect(x-cw/2, Math.min(yo,yc), cw, Math.max(1,Math.abs(yc-yo))); }
  } else {
    const grd=ctx.createLinearGradient(0,padT,0,padT+plotH);
    grd.addColorStop(0,col+"2e"); grd.addColorStop(1,col+"00");
    ctx.beginPath(); ctx.moveTo(X(0),padT+plotH);
    for(let k=0;k<n;k++) ctx.lineTo(X(k),Y(valOf(vis[k])));
    ctx.lineTo(X(n-1),padT+plotH); ctx.closePath(); ctx.fillStyle=grd; ctx.fill();
    ctx.beginPath(); ctx.moveTo(X(0),Y(valOf(vis[0])));
    for(let k=1;k<n;k++) ctx.lineTo(X(k),Y(valOf(vis[k])));
    ctx.strokeStyle=col; ctx.lineWidth=1.7; ctx.lineJoin="round"; ctx.stroke();
    if(n===1){ ctx.fillStyle=col; ctx.beginPath(); ctx.arc(X(0),Y(valOf(vis[0])),3,0,7); ctx.fill(); }
  }

  (function(){ const val=valOf(last), y=Y(val), t=fmtVal(val); ctx.font="bold 10px Segoe UI,Arial";
    const lc = mode==="hist" ? (val>=0?COL_LONG:COL_SHORT) : col;
    const tw=ctx.measureText(t).width, bx=Math.min(X(n-1), W-padR-tw/2-3);
    ctx.fillStyle=lc; roundRect(ctx,bx-tw/2-6,y-8,tw+12,16,4); ctx.fill();
    ctx.fillStyle="#06100a"; ctx.textAlign="center"; ctx.textBaseline="middle"; ctx.fillText(t,bx,y); })();

  const hi=RG[i].hoverIdx, tip=card.querySelector(".tip");
  if(hi!=null && hi>=0 && hi<n){
    const c=vis[hi], x=X(hi), y=Y(valOf(c));
    ctx.save(); ctx.strokeStyle="#36465a"; ctx.setLineDash([4,4]);
    ctx.beginPath(); ctx.moveTo(x,padT); ctx.lineTo(x,padT+plotH); ctx.stroke(); ctx.setLineDash([]);
    const dc = mode==="hist" ? (valOf(c)>=0?COL_LONG:COL_SHORT) : col;
    ctx.fillStyle=dc; ctx.beginPath(); ctx.arc(x,y,3.5,0,7); ctx.fill();
    ctx.strokeStyle="#fff"; ctx.lineWidth=1.2; ctx.stroke(); ctx.restore();
    let bodyHtml;
    if(w.src==="flow" && w.metric==="disb")
      bodyHtml = `<span style="color:${dc}">${ttl} ${fmtVal(valOf(c))}</span><br>▲ покуп ${fmt(c.vb)} · ▼ прод ${fmt(c.vs)}`;
    else if(w.src==="flow")
      bodyHtml = `<span style="color:${col}">${ttl} ${fmt(c.c)}</span> · цена ${c.close}`
        + (mode==="candle"?`<br>О ${fmt(c.o)} · Макс ${fmt(c.h)} · Мин ${fmt(c.l)}`:``);
    else
      bodyHtml = `<span style="color:${col}">${ttl} ${fmt(c.c)}</span>`;
    tip.style.opacity=1;
    tip.innerHTML = `${intraday?"":dmy(c.date)+" "}${c.time.slice(0,5)}<br>${bodyHtml}`;
    let tx=x+12; if(tx+tip.offsetWidth>W) tx=x-tip.offsetWidth-12; if(tx<2)tx=2;
    tip.style.left=tx+"px"; tip.style.top=(padT+2)+"px";
  } else { tip.style.opacity=0; }
}

// ── панель управления ───────────────────────────────────────────────────────────
function applyTimer(){ clearInterval(state.timer);
  if(state.auto) state.timer=setInterval(loadAll, state.ms); }
function setAuto(on){ state.auto=on; $("#autoBtn").classList.toggle("on",on); applyTimer(); saveState(); }

$("#wMinus").onclick = ()=> setCount(state.wins.length-1);
$("#wPlus").onclick  = ()=> setCount(state.wins.length+1);
$("#refresh").onclick = loadAll;
$("#autoBtn").onclick = ()=> setAuto(!state.auto);
let rt; window.addEventListener("resize", ()=>{ clearTimeout(rt);
  rt=setTimeout(()=>{ for(let i=0;i<state.wins.length;i++) drawWindow(i); }, 140); });

$("#autoBtn").classList.toggle("on", state.auto);
drawAll(); applyTimer(); loadAll();
</script>
</body>
</html>"""


def lan_ip():
    """Лучший «сетевой» IPv4 этого ПК (для доступа с телефона). Исключаем 127.* и 169.254.*;
    предпочитаем 192.168.*, затем 10.*, затем 172.16–31 (но 172.17/18 — обычно docker/туннели)."""
    cands = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            cands.add(info[4][0])
    except Exception:
        pass
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        cands.add(s.getsockname()[0])
        s.close()
    except Exception:
        pass
    cands = [c for c in cands if not c.startswith("127.") and not c.startswith("169.254.")]

    def score(ip):
        if ip.startswith("192.168."):
            return 0
        if ip.startswith("10."):
            return 1
        if ip.startswith("172."):
            try:
                o = int(ip.split(".")[1])
                if 16 <= o <= 31:
                    return 4 if o in (17, 18) else 2
            except Exception:
                pass
        return 3
    cands.sort(key=score)
    return cands[0] if cands else None


def _server_alive(port):
    """Уже работает наш сервер на этом порту? (чтобы не плодить копии при автозапуске)"""
    try:
        with urllib.request.urlopen("http://127.0.0.1:%d/" % port, timeout=1.5) as r:
            return r.status == 200 and "Скринер" in r.read(4000).decode("utf-8", "replace")
    except Exception:
        return False


def open_browser(url):
    """Открыть скринер в браузере, который НЕ блокирует localhost.
    В Chrome пользователя стоит расширение, блокирующее localhost/127.0.0.1,
    поэтому предпочитаем Microsoft Edge (он есть на любом Win10/11 и чистый)."""
    for exe in (
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    ):
        if os.path.exists(exe):
            try:
                subprocess.Popen([exe, url])
                return
            except Exception:
                pass
    try:                                        # протокол microsoft-edge: (есть всегда)
        os.startfile("microsoft-edge:" + url)
        return
    except Exception:
        pass
    try:                                        # крайний случай — браузер по умолчанию
        webbrowser.open(url)
    except Exception:
        pass


def ensure_autostart():
    """Один раз создаёт ярлык в «Автозагрузке», чтобы при включении ПК скринер
    сам поднимался в сети (для телефона). Тихо, не критично, цель — python.exe
    (он уже разрешён в брандмауэре), окно свёрнуто, ключ --service. Возвращает текст статуса."""
    try:
        startup = os.path.join(os.environ["APPDATA"],
                               r"Microsoft\Windows\Start Menu\Programs\Startup")
        lnk = os.path.join(startup, "OI_Screener.lnk")
        if os.path.exists(lnk):
            return "включён"
        pyexe = sys.executable                  # ...\python.exe (с правилом в брандмауэре)
        target = os.path.join(HERE, "oi_live.py")
        ps = (
            "$s=(New-Object -ComObject WScript.Shell).CreateShortcut('%s');"
            "$s.TargetPath='%s';$s.Arguments='\"%s\" --service';"
            "$s.WorkingDirectory='%s';$s.WindowStyle=7;$s.Description='OI Screener';$s.Save()"
        ) % (lnk, pyexe, target, HERE)
        subprocess.run(["powershell", "-NoProfile", "-Command", ps],
                       timeout=20, capture_output=True)
        return "включён" if os.path.exists(lnk) else "не удалось (не критично)"
    except Exception:
        return "не удалось (не критично)"


def main():
    global PORT
    cloud = bool(os.environ.get("PORT"))        # облачный хост задаёт $PORT -> без браузера/автозапуска
    service = ("--service" in sys.argv) or cloud   # тихо, без браузера
    no_browser = service or bool(os.environ.get("OI_NO_BROWSER"))

    if _server_alive(PORT):                     # уже запущен (напр. из автозагрузки)
        print("Скринер уже работает: http://127.0.0.1:%d/" % PORT)
        if not no_browser:
            open_browser("http://127.0.0.1:%d/" % PORT)
        return

    srv = None
    for p in range(PORT, PORT + 25):
        try:
            srv = ThreadingHTTPServer(("0.0.0.0", p), Handler)   # все интерфейсы → видно по сети
            PORT = p
            break
        except OSError:
            continue
    if not srv:
        print("Не нашёл свободный порт.")
        return

    local = "http://127.0.0.1:%d/" % PORT
    ip = lan_ip()
    net = "http://%s:%d/" % (ip, PORT) if ip else None
    tok = read_token()
    try:
        auto = ensure_autostart() if not service else "включён (сервис/облако)"
    except Exception:
        auto = "—"
    print("=" * 58)
    print(" Скринер открытого интереса — LIVE (валютные фьючи)")
    print(" Этот ПК  :", local)
    if net:
        print(" По сети  :", net)
        print("            ^ открой этот адрес на телефоне (та же Wi-Fi)")
    print(" Токен    :", "найден" if tok else "НЕ найден -> " + TOKEN_FILE)
    print(" Автозапуск при включении ПК:", auto)
    print(" Стоп     : закрой это окно или нажми Ctrl+C")
    print("=" * 58)
    if not no_browser:
        threading.Timer(0.8, lambda: open_browser(local)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nОстановлено.")


if __name__ == "__main__":
    main()
