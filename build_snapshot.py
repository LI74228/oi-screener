# -*- coding: utf-8 -*-
"""Генератор статического снимка скринера для GitHub Pages.

GitHub сам сервер не крутит, но его планировщик (Actions cron) по расписанию
запускает этот скрипт: он тянет данные MOEX (тем же кодом, что и живой сервер —
импортируя oi_live.py) и раскладывает ГОТОВЫЕ ответы API в файлы в docs/.
GitHub Pages отдаёт docs/ как обычный сайт — открывается с любого устройства,
ПК выключен, Render не нужен. Токен живёт только в секрете Actions, в сайт не попадает.

Кладём:
  docs/index.html                  — тот же интерфейс, что и у сервера (oi_live.HTML) + шим,
                                     перенаправляющий /api/... на статические файлы
  docs/all.json                    — ответ /api/all за ~месяц (клиент режет по периоду сам)
  docs/series_{code}_{src}.json    — ответы /api/series (flow/conc) по каждому инструменту
  docs/meta.json                   — когда снят снимок
"""
import os, sys, json, datetime
sys.stdout.reconfigure(encoding="utf-8")

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.join(HERE, "docs")
os.makedirs(DOCS, exist_ok=True)

import oi_live  # безопасно: сервер стартует только под __main__

TODAY = datetime.date.today()
# окно снимка: ~2 недели. Скальперу нужны Сегодня/3 дня/Неделя (их отдаём целиком);
# «Месяц» в снимке будет частичным. Меньше окно = лёгкий файл (моб.) и быстрый прогон.
FRM = (TODAY - datetime.timedelta(days=16)).isoformat()
TILL = TODAY.isoformat()


def write(name, obj):
    p = os.path.join(DOCS, name)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    print("  ->", name, "(%d Б)" % os.path.getsize(p))


# 1) futoi — все инструменты за месяц
print("futoi /api/all %s..%s" % (FRM, TILL))
allj = oi_live.fetch_all(FRM, TILL)
write("all.json", allj)

# 2) AlgoPack «поток»/«концентрация» по каждому инструменту (клиент грузит лениво -> кладём все)
for code in oi_live.INSTRUMENTS:
    for src in ("flow", "conc"):
        try:
            res = oi_live.get_series(src, code, FRM, TILL)
        except Exception as e:
            res = {"source": "error", "message": str(e), "columns": [], "data": []}
        write("series_%s_%s.json" % (code, src), res)

# 3) статическая страница = живой интерфейс + шим на статические файлы
SHIM = """<script>
(function(){
  var _f = window.fetch.bind(window);
  window.fetch = function(u, o){
    try{
      var s = String(typeof u==='string'? u : (u&&u.url)||'');
      if(s.indexOf('/api/all')>=0) return _f('./all.json',{cache:'no-store'});
      var m = s.match(/\\/api\\/series\\?(.*)$/);
      if(m){ var q=new URLSearchParams(m[1]);
        return _f('./series_'+q.get('code')+'_'+q.get('src')+'.json',{cache:'no-store'}); }
    }catch(e){}
    return _f(u,o);
  };
  fetch('./meta.json',{cache:'no-store'}).then(function(r){return r.json();}).then(function(mm){
    if(mm&&mm.generated){ document.title='Скринер ОИ · снимок '+mm.generated;
      var t=document.getElementById('srcTxt');
      if(t){ var o=t.textContent; setTimeout(function(){ t.title='снимок MOEX от '+mm.generated+' (обновляется автоматически)'; },1500); }
    }
  }).catch(function(){});
})();
</script>
"""
html = oi_live.HTML
# вставляем шим перед основным скриптом приложения
marker = "<script>"
html = html.replace(marker, SHIM + marker, 1)
with open(os.path.join(DOCS, "index.html"), "w", encoding="utf-8") as f:
    f.write(html)
print("  -> index.html (%d Б)" % os.path.getsize(os.path.join(DOCS, "index.html")))

# 4) мета
meta = {"generated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "frm": FRM, "till": TILL, "instruments": oi_live.INSTRUMENTS}
write("meta.json", meta)

# .nojekyll — чтобы Pages не пытался обрабатывать сайт Jekyll'ом
open(os.path.join(DOCS, ".nojekyll"), "w").close()
print("DONE")
