"""Send a short Apprise message using the URL from the web UI settings.json (never prints the URL).
Usage: python tools/notify.py "Title" "Body"   (CONFIG_DIR defaults to /config)"""
import json, os, sys

def main():
    cfg = os.environ.get('CONFIG_DIR', '/config')
    with open(os.path.join(cfg, 'settings.json'), encoding='utf-8') as f:
        url = (json.load(f).get('apprise_url') or '').strip()
    if not url:
        print('no apprise_url configured'); return 1
    import apprise
    a = apprise.Apprise(); a.add(url)
    ok = bool(a.notify(title=sys.argv[1], body=sys.argv[2]))
    print('sent' if ok else 'failed')
    return 0 if ok else 2

if __name__ == '__main__':
    sys.exit(main())
