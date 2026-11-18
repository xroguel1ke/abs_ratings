#!/bin/bash
# Screenshots of the web UI with the dev code: a COPY of the real config (password 'test', no schedule / quick mode, DRY_RUN),
# ABS = fake ABS from a snapshot -> nothing can reach the real ABS. Output: /tmp/claude-0/uishots/*.png (private data: never commit).
# Usage: tools/ui_shots.sh [snapshot] [port]
DEV=/mnt/user/appdata/abs_ratings_dev; SNAP=${1:-/src/tests/private/abs_snapshot_20261115c.json}; PORT=${2:-47899}; FPORT=$((PORT+1))
W=/tmp/claude-0/uishots; rm -rf $W; mkdir -p $W/cfg $W/out
rsync -a --exclude logs --exclude cover_backup --exclude debug_logs --exclude '*.bak*' /mnt/user/appdata/audiobookshelf/abs_scripts/ $W/cfg/
docker run --rm -v $DEV:/src:ro -v $W/cfg:/config -w /src -e CONFIG_DIR=/config --entrypoint python ghcr.io/xroguel1ke/abs_ratings:web-ui -c "
import sys; sys.path.insert(0,'/src'); from webui import core
s=core.load_settings(); s.update({'ABS_URL':'http://127.0.0.1:$FPORT','API_TOKEN':'fake','schedule_enabled':False,'QUICK_MODE_MINUTES':0,'DRY_RUN':True,'apprise_url':'','GOODREADS_ENABLED':False,'GCAL_ENABLED':False})
core.save_settings(s); core.set_password(core.load_settings(), 'test')"
docker rm -f abs_ratings_test_uishot >/dev/null 2>&1
docker run -d --rm --name abs_ratings_test_uishot --network host --cpuset-cpus=0-9,12-31 -e PYTHONDONTWRITEBYTECODE=1 -v $DEV:/src:ro -v $W/cfg:/config -w /src \
  -e CONFIG_DIR=/config -e PORT=$PORT --entrypoint sh ghcr.io/xroguel1ke/abs_ratings:web-ui -c \
  "python -I tools/fake_abs.py --snapshot $SNAP --port $FPORT >/dev/null 2>&1 & sleep 3; python -m webui.app" >/dev/null
for i in $(seq 1 30); do curl -fsS http://127.0.0.1:$PORT/health >/dev/null 2>&1 && break; sleep 1; done
cat > $W/shots.py <<PY
from playwright.sync_api import sync_playwright
import sys, os
FULL = os.environ.get('FULL', '1') == '1'
B='http://127.0.0.1:$PORT'
pages=[a.split('=',1) for a in sys.argv[1:]]
with sync_playwright() as p:
    br=p.chromium.launch()
    for vw,suffix in (({'width':1400,'height':900},''),({'width':390,'height':844},'_mobile')):
        pg=br.new_page(viewport=vw); pg.add_init_script("try{localStorage.setItem('abs_tour_done','1')}catch(e){}")
        pg.goto(B+'/login'); pg.fill('input[type=password]','test'); pg.keyboard.press('Enter'); pg.wait_for_load_state('networkidle')
        for name,path in pages:
            if suffix and not name.endswith('M'): continue
            pg.goto(B+path); pg.wait_for_load_state('networkidle'); pg.wait_for_timeout(500)
            pg.screenshot(path=f'/out/{name.rstrip("M")}{suffix}.png', full_page=FULL)
    br.close()
PY
docker run --rm --network host -e FULL=${FULL:-1} -v $W/out:/out -v $W/shots.py:/shots.py:ro mcr.microsoft.com/playwright/python:v1.47.0-jammy sh -c "pip install -q playwright==1.47.0 >/dev/null 2>&1; python /shots.py \"\$@\"" sh "${@:3}"
docker stop abs_ratings_test_uishot >/dev/null
ls $W/out
