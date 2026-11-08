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
    var step = parseInt(store.get('abs_tour_step') || '', 10);
    if (document.querySelector('[data-tour]') && !isNaN(step)) setTimeout(function () { window.ABSTour.resume(step); }, 300);
    else if (document.querySelector('[data-tour]') && !store.get('abs_tour_done')) setTimeout(function () { window.ABSTour.start(); }, 400);
  });

  // ---- onboarding tour ----
  var STEPS = [
    { page: 'dashboard', sel: null, t: T('Welcome to ABS Ratings'), p: T('ABS Ratings writes Audible and Hardcover ratings into your book descriptions and repairs metadata. This tour walks through every page and points at the important parts. You can restart it under Settings.') },
    { page: 'dashboard', sel: '.hero', t: T('Overview: at a glance'), p: T('What waits for your decision, the last scan and the next scheduled one. A click on a card opens the page behind it.') },
    { page: 'dashboard', sel: '#health', t: T('Library check'), p: T('After every scan the whole library is checked: books without ASIN, an ASIN on several books, series names in another language, folder and volume that disagree. Orange = something to look at; open a line to see the books.') },
    { page: 'dashboard', sel: '#coverage', t: T('Coverage'), p: T('How many books have each field (ASIN, series, narrators, cover …) and how that changed. "without" lists the books and can check them again in a test run.') },
    { page: 'runs', sel: '#startform', t: T('Start a scan'), p: T('Choose the tasks (metadata & ratings, authors, series …) and start. Keep "test run" ticked at first: nothing is written, you see every planned change and apply only what you want.') },
    { page: 'runs', sel: '.tabs', t: T('Scans and changes written'), p: T('The list of all scans with live progress (time left, expected end, changes so far). The second tab shows what was really written to ABS.') },
    { page: 'history', sel: '.card.tablewrap', t: T('Undo'), p: T('Every written change, per scan. Open a scan to undo one field, one book or the whole scan; replaced covers come back from the backup.') },
    { page: 'books', sel: 'form.card', t: T('Books'), p: T('Every checked book. Search, filter by status and sort by certainty - how sure the script is that the ASIN is exactly this book.') },
    { page: 'books', sel: '.card.tablewrap', t: T('Book page'), p: T('A click on a title shows ABS next to Audible and Hardcover, the reasoning of every decision, and lets you set an ASIN by hand or test this one book.') },
    { page: 'review', sel: '.snav', t: T('Review list'), p: T('Only what the script could not verify on its own, in sections: books, series, authors. Done entries disappear by themselves after the next scan.') },
    { page: 'review', sel: '.spanel', t: T('Deciding'), p: T('Each suggestion shows ABS next to the suggestion - green dots agree, orange ones differ. Apply or discard; "Apply all" / "Discard all" handles every entry of one kind at once.') },
    { page: 'settings', sel: '.snav', t: T('Settings'), p: T('Sections on the left: connection, run and schedule, sources (Audible, Hardcover, Goodreads), metadata and rating block, series & authors, notifications, language. Each section has its own save button.') },
    { page: 'settings', sel: null, t: T('Protect what you curated'), p: T('Lock tags in ABS keep the script away from a field: lock_series, lock_title, lock_asin, lock_authors … or lock_all for the whole book. Everything else the script may correct.') },
    { page: 'settings', sel: '#theme-toggle', t: T('Design'), p: T('Light or dark. On the first visit the system setting is used, then the browser remembers your choice.') }
  ];
  var i = 0, dim = null, box = null, hl = null;
  function pageUrl(pg) {
    var a = document.querySelector('a[data-tour=' + (pg === 'history' ? 'runs' : pg) + ']'); if (!a) return null;
    var h = a.getAttribute('href'); return pg === 'history' ? h.replace(/runs\/?$/, 'history') : h;
  }
  function onPage(pg) { var u = pageUrl(pg); return u && (location.pathname === u || location.pathname === u.replace(/\/$/, '')); }
  function clear() { if (hl) hl.classList.remove('tour-hl'); hl = null; }
  function end() { clear(); if (dim) dim.remove(); if (box) box.remove(); dim = box = null; store.set('abs_tour_done', '1'); store.set('abs_tour_step', ''); }
  function go(n) {
    i = n; var s = STEPS[i];
    if (!onPage(s.page)) { var u = pageUrl(s.page); if (u) { store.set('abs_tour_step', String(i)); location.href = u; return; } }
    show();
  }
  function show() {
    clear();
    var s = STEPS[i], el = s.sel ? document.querySelector(s.sel) : null;
    if (s.sel && !el) { if (i < STEPS.length - 1) return go(i + 1); return end(); }
    if (el) { el.classList.add('tour-hl'); hl = el; el.scrollIntoView({block: 'center', behavior: 'smooth'}); }
    box.innerHTML = '<h3></h3><p></p><div class="row"><button type="button" data-a="skip">' + T('Skip') + '</button>' +
      '<span class="row">' + (i > 0 ? '<button type="button" data-a="back">' + T('Back') + '</button>' : '') +
      '<button type="button" class="primary" data-a="next">' + (i === STEPS.length - 1 ? T('Done') : T('Next')) + '</button></span></div>' +
      '<div class="muted small" style="margin-top:8px">' + (i + 1) + ' / ' + STEPS.length + '</div>';
    box.querySelector('h3').textContent = s.t; box.querySelector('p').textContent = s.p;
    setTimeout(function () {
      var r = el ? el.getBoundingClientRect() : null, w = Math.min(360, window.innerWidth - 24);
      box.style.width = w + 'px';
      if (r) {
        var below = r.bottom + 12, top = below + 220 < window.innerHeight ? below : Math.max(12, r.top - 232);
        box.style.top = top + 'px'; box.style.left = Math.max(12, Math.min(r.left, window.innerWidth - w - 12)) + 'px';
      } else { box.style.top = '20vh'; box.style.left = Math.max(12, (window.innerWidth - w) / 2) + 'px'; }
    }, el ? 350 : 0);
  }
  function open() {
    dim = document.createElement('div'); dim.className = 'tour-dim'; document.body.appendChild(dim);
    box = document.createElement('div'); box.className = 'tour-box'; box.setAttribute('role', 'dialog'); document.body.appendChild(box);
    box.addEventListener('click', function (e) {
      var a = e.target.getAttribute('data-a');
      if (a === 'skip') end();
      else if (a === 'back') go(Math.max(0, i - 1));
      else if (a === 'next') { if (i === STEPS.length - 1) end(); else go(i + 1); }
    });
    dim.addEventListener('click', end);
  }
  var Tour = {
    start: function () { end(); store.set('abs_tour_done', ''); open(); go(0); },
    resume: function (n) { open(); i = n; show(); }
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
