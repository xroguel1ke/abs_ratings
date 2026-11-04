// ABS Ratings web UI: theme toggle (auto / light / dark) and the onboarding tour. No external libraries.
(function () {
  var store = {
    get: function (k) { try { return localStorage.getItem(k); } catch (e) { return null; } },
    set: function (k, v) { try { localStorage.setItem(k, v); } catch (e) {} }
  };

  // ---- theme ----
  var THEMES = ['auto', 'light', 'dark'], ICON = { auto: '◐', light: '☀', dark: '☾' }, LABEL = { auto: 'Design: automatisch', light: 'Design: hell', dark: 'Design: dunkel' };
  function applyTheme(t) {
    if (t === 'light' || t === 'dark') document.documentElement.setAttribute('data-theme', t);
    else document.documentElement.removeAttribute('data-theme');
    var b = document.getElementById('theme-toggle');
    if (b) { b.textContent = ICON[t] || ICON.auto; b.title = LABEL[t] || LABEL.auto; }
  }
  var theme = store.get('abs_theme') || 'auto';
  applyTheme(theme);
  document.addEventListener('DOMContentLoaded', function () {
    applyTheme(theme);
    var b = document.getElementById('theme-toggle');
    if (b) b.addEventListener('click', function () {
      theme = THEMES[(THEMES.indexOf(theme) + 1) % THEMES.length];
      store.set('abs_theme', theme); applyTheme(theme);
    });
    if (document.querySelector('[data-tour]') && !store.get('abs_tour_done')) setTimeout(function () { Tour.start(); }, 400);
  });

  // ---- onboarding tour ----
  var STEPS = [
    { sel: null, t: 'Willkommen bei ABS Ratings', p: 'Kurzer Rundgang in 8 Schritten. Du kannst ihn jederzeit überspringen und unter Einstellungen neu starten.' },
    { sel: '[data-tour=dashboard]', t: 'Dashboard', p: 'Überblick nach jedem Lauf: Bücher mit Bewertungen, Probleme, die Prüfliste (Vorschläge, die du bestätigst) und zusammengeführte Reihen/Autoren.' },
    { sel: '[data-tour=books]', t: 'Bücher', p: 'Alle geprüften Bücher, sortiert nach Sicherheit des Treffers. Mit Suche, Vergleich ABS ↔ Audible und der Begründung jeder Entscheidung.' },
    { sel: '[data-tour=status]', t: 'Status', p: 'Zustand der Quellen (Audible, Hardcover, Goodreads) und was gerade läuft.' },
    { sel: '[data-tour=runs]', t: 'Läufe', p: 'Jeder Lauf mit Fortschritt und geplanten Änderungen. Starte neue Läufe immer zuerst als Testlauf: dann wird nichts geschrieben.' },
    { sel: '[data-tour=history]', t: 'Verlauf', p: 'Alles, was wirklich in ABS geschrieben wurde - pro Lauf, Buch und Feld. Jede Änderung lässt sich einzeln, pro Buch oder für den ganzen Lauf rückgängig machen.' },
    { sel: '[data-tour=settings]', t: 'Einstellungen', p: 'ABS-Verbindung, Zeitplan, Quellen und Benachrichtigungen. Hier startest du auch diese Tour neu.' },
    { sel: '#theme-toggle', t: 'Design', p: 'Hell, dunkel oder automatisch nach System. Ein Klick wechselt.' }
  ];
  var i = 0, dim = null, box = null, hl = null;
  function clear() { if (hl) hl.classList.remove('tour-hl'); hl = null; }
  function end() { clear(); if (dim) dim.remove(); if (box) box.remove(); dim = box = null; store.set('abs_tour_done', '1'); }
  function show() {
    clear();
    var s = STEPS[i], el = s.sel ? document.querySelector(s.sel) : null;
    if (s.sel && !el) { if (i < STEPS.length - 1) { i++; return show(); } return end(); }
    if (el) { el.classList.add('tour-hl'); hl = el; }
    box.innerHTML = '<h3></h3><p></p><div class="row"><button type="button" data-a="skip">Überspringen</button>' +
      '<span class="row">' + (i > 0 ? '<button type="button" data-a="back">Zurück</button>' : '') +
      '<button type="button" class="primary" data-a="next">' + (i === STEPS.length - 1 ? 'Fertig' : 'Weiter') + '</button></span></div>' +
      '<div class="muted small" style="margin-top:8px">' + (i + 1) + ' / ' + STEPS.length + '</div>';
    box.querySelector('h3').textContent = s.t; box.querySelector('p').textContent = s.p;
    var r = el ? el.getBoundingClientRect() : null, w = Math.min(340, window.innerWidth - 24);
    box.style.width = w + 'px';
    if (r) { box.style.top = Math.min(r.bottom + 12, window.innerHeight - 200) + 'px'; box.style.left = Math.max(12, Math.min(r.left, window.innerWidth - w - 12)) + 'px'; }
    else { box.style.top = '20vh'; box.style.left = Math.max(12, (window.innerWidth - w) / 2) + 'px'; }
  }
  var Tour = {
    start: function () {
      end(); i = 0;
      dim = document.createElement('div'); dim.className = 'tour-dim'; document.body.appendChild(dim);
      box = document.createElement('div'); box.className = 'tour-box'; box.setAttribute('role', 'dialog'); document.body.appendChild(box);
      box.addEventListener('click', function (e) {
        var a = e.target.getAttribute('data-a');
        if (a === 'skip') end();
        else if (a === 'back') { i = Math.max(0, i - 1); show(); }
        else if (a === 'next') { if (i === STEPS.length - 1) end(); else { i++; show(); } }
      });
      dim.addEventListener('click', end);
      show();
    }
  };
  window.ABSTour = Tour;
})();
