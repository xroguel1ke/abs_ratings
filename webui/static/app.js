// ABS Ratings web UI: theme switch (light / dark), onboarding tour, confirm dialog. No external libraries.
// Texts are English; window.I18N (set by base.html) holds the German translation when the UI language is German.
function T(s) { return (window.I18N && window.I18N[s]) || s; }
(function () {
  var store = {
    get: function (k) { try { return localStorage.getItem(k); } catch (e) { return null; } },
    set: function (k, v) { try { localStorage.setItem(k, v); } catch (e) {} }
  };

  // ---- theme ----
  // first visit: follow the system setting (not stored); the switch toggles light <-> dark and remembers the choice in this browser
  var ICON = window.ICONS || { light: '', dark: '' }, LABEL = { light: T('Switch to dark'), dark: T('Switch to light') };
  function systemTheme() { try { return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'; } catch (e) { return 'light'; } }
  function applyTheme(t) {
    document.documentElement.setAttribute('data-theme', t);
    var b = document.getElementById('theme-toggle');
    if (b) { b.innerHTML = ICON[t]; b.title = LABEL[t]; b.setAttribute('aria-label', LABEL[t]); }
  }
  var saved = store.get('abs_theme');
  var theme = (saved === 'light' || saved === 'dark') ? saved : systemTheme();  // an old 'auto' value counts as not set
  applyTheme(theme);
  document.addEventListener('DOMContentLoaded', function () {
    applyTheme(theme);
    var b = document.getElementById('theme-toggle');
    if (b) b.addEventListener('click', function () {
      theme = theme === 'dark' ? 'light' : 'dark';
      store.set('abs_theme', theme); applyTheme(theme);
    });
    if (document.querySelector('[data-tour]') && !store.get('abs_tour_done')) setTimeout(function () { Tour.start(); }, 400);
  });

  // ---- onboarding tour ----
  var STEPS = [
    { sel: null, t: T('Welcome to ABS Ratings'), p: T('ABS Ratings writes Audible and Hardcover ratings into your book descriptions and repairs metadata: ASIN, series, authors, titles, covers. This tour shows the pages (10 steps). You can restart it under Settings.') },
    { sel: '[data-tour=dashboard]', t: T('Overview'), p: T('Your start page: what needs a decision, the last and the next scan, the library tiles (click one to filter), the library check after every scan and the coverage of every field.') },
    { sel: '[data-tour=dashboard]', t: T('Library check'), p: T('Orange dots mark problems the script found but could not fix safely: missing ASIN, an ASIN on several books, series names in another language, folder and volume that disagree. Open a line to see the books.') },
    { sel: '[data-tour=runs]', t: T('Scans'), p: T('Start a scan here and follow it live: progress, time left, expected end and what changed. Start with a test run: nothing is written, you review the planned changes and apply only what you want.') },
    { sel: '[data-tour=runs]', t: T('Changes written'), p: T('The second tab of Scans lists everything that was really written to ABS. Every change can be undone - one field, one book or a whole scan; replaced covers come back from the backup.') },
    { sel: '[data-tour=books]', t: T('Books'), p: T('Every checked book with its status and how certain the match is. A book page shows ABS next to Audible and Hardcover, why each decision was made, and lets you set an ASIN by hand or test one book.') },
    { sel: '[data-tour=review]', t: T('Review list'), p: T('Only what the script could not verify on its own. Each suggestion shows ABS next to the suggestion (title, authors, narrators, runtime, series) - green dots agree. Apply or discard, also for all entries of one kind at once. Done entries disappear by themselves.') },
    { sel: '[data-tour=settings]', t: T('Settings'), p: T('Connection to ABS, schedule, sources (Audible, Hardcover, Goodreads), how the rating block looks, series and author rules, notifications and the language.') },
    { sel: null, t: T('Protect what you curated'), p: T('Lock tags in ABS keep the script away from a field: lock_series, lock_title, lock_asin, lock_authors ... or lock_all for the whole book. Everything else the script may correct.') },
    { sel: '#theme-toggle', t: T('Design'), p: T('Light or dark. On the first visit the system setting is used, then the browser remembers your choice.') }
  ];
  var i = 0, dim = null, box = null, hl = null;
  function clear() { if (hl) hl.classList.remove('tour-hl'); hl = null; }
  function end() { clear(); if (dim) dim.remove(); if (box) box.remove(); dim = box = null; store.set('abs_tour_done', '1'); }
  function show() {
    clear();
    var s = STEPS[i], el = s.sel ? document.querySelector(s.sel) : null;
    if (s.sel && !el) { if (i < STEPS.length - 1) { i++; return show(); } return end(); }
    if (el) { el.classList.add('tour-hl'); hl = el; }
    box.innerHTML = '<h3></h3><p></p><div class="row"><button type="button" data-a="skip">' + T('Skip') + '</button>' +
      '<span class="row">' + (i > 0 ? '<button type="button" data-a="back">' + T('Back') + '</button>' : '') +
      '<button type="button" class="primary" data-a="next">' + (i === STEPS.length - 1 ? T('Done') : T('Next')) + '</button></span></div>' +
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

// ---- confirmation dialog (replaces the browser's confirm()): forms or buttons with data-confirm="question" ----
// data-confirm-ok="Button text" optional; dangerous actions (undo, real run, write) get a red confirm button automatically.
(function () {
  function dialog(msg, okText, danger) {
    return new Promise(function (resolve) {
      var back = document.createElement('div'); back.className = 'modal-back';
      back.innerHTML = '<div class="modal" role="dialog" aria-modal="true"><h3>' + T('Please confirm') + '</h3><p></p>' +
        '<div class="row" style="justify-content:flex-end"><button type="button" data-a="no">' + T('Cancel') + '</button>' +
        '<button type="button" data-a="yes" class="' + (danger ? 'primary danger-fill' : 'primary') + '"></button></div></div>';
      back.querySelector('p').textContent = msg;
      back.querySelector('[data-a=yes]').textContent = T(okText || 'Confirm');
      function close(v) { document.removeEventListener('keydown', key); back.remove(); resolve(v); }
      function key(e) { if (e.key === 'Escape') close(false); if (e.key === 'Enter') close(true); }
      back.addEventListener('click', function (e) { var a = e.target.getAttribute('data-a'); if (a) close(a === 'yes'); else if (e.target === back) close(false); });
      document.addEventListener('keydown', key);
      document.body.appendChild(back); back.querySelector('[data-a=no]').focus();
    });
  }
  window.ABSConfirm = dialog;
  document.addEventListener('submit', function (e) {
    var f = e.target, b = e.submitter;
    var msg = (b && b.getAttribute('data-confirm')) || f.getAttribute('data-confirm');
    if (!msg || f.dataset.confirmed) return;
    e.preventDefault();
    var danger = /undo|rückgängig|real scan|echter|really|wirklich|delete|löschen|write|schreiben/i.test(msg) || (b && b.classList.contains('danger'));
    var ok = (b && b.getAttribute('data-confirm-ok')) || f.getAttribute('data-confirm-ok');
    dialog(T(msg), ok, danger).then(function (yes) {
      if (!yes) return;
      f.dataset.confirmed = '1';
      if (f.requestSubmit) f.requestSubmit(b || undefined); else f.submit();
    });
  });
})();
