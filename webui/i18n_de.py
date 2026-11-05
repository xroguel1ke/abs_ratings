"""German translation of the web UI (English source text -> German). Missing entries are shown in English."""
from webui.i18n import add

# --- settings: field labels + help, groups, sections, providers (webui/core.py FIELDS/SECTIONS/PROVIDERS) ---
add({
    'Connection': 'Verbindung', 'Run': 'Lauf', 'Sources': 'Quellen', 'Metadata': 'Metadaten', 'Series & authors': 'Reihen & Autoren',
    'Logs': 'Logs', 'Schedule': 'Zeitplan', 'Notifications': 'Benachrichtigungen', 'Interface': 'Oberfläche',
    'Audiobookshelf URL': 'Audiobookshelf-URL', 'e.g. http://192.168.50.16:13378': 'z.B. http://192.168.50.16:13378',
    'ABS API token': 'ABS-API-Token', 'ABS: Settings → Users → API token': 'ABS: Einstellungen → Benutzer → API-Token',
    'Libraries': 'Bibliotheken', 'Which ABS libraries are processed': 'Welche ABS-Bibliotheken bearbeitet werden',
    'ABS timeout (s)': 'ABS-Timeout (s)',
    'Test run (DRY_RUN) for scheduled scans': 'Testlauf (DRY_RUN) für geplante Scans',
    'On = nothing is written to ABS, changes are only planned and logged': 'An = nichts wird in ABS geschrieben, Änderungen werden nur geplant und protokolliert',
    'Books per scan': 'Bücher pro Scan', 'Picked at random from all due books of all libraries': 'Zufällig aus allen fälligen Büchern aller Bibliotheken',
    'Refresh after (days)': 'Auffrischen nach (Tage)', '0 = every book is due (one complete pass)': '0 = jedes Buch ist fällig (ein kompletter Durchlauf)',
    'ABS backup before real scans': 'ABS-Backup vor echten Scans',
    'Creates an ABS backup before every scan without DRY_RUN; if it fails nothing is written': 'Legt vor jedem Scan ohne DRY_RUN ein ABS-Backup an; schlägt es fehl, wird nichts geschrieben',
    'Extra pause between books (s)': 'Zusätzliche Pause zwischen Büchern (s)',
    'Every source has its own minimum interval, so 0 is safe and fastest': 'Jede Quelle hat ihren eigenen Mindestabstand, 0 ist daher sicher und am schnellsten',
    'Re-check books without ratings (days)': 'Bücher ohne Bewertungen erneut prüfen (Tage)',
    'Log level': 'Log-Level', 'DEBUG only for troubleshooting (much bigger logs)': 'DEBUG nur zur Fehlersuche (viel größere Logs)',
    'Keep logs (days)': 'Logs aufbewahren (Tage)', 'Older logs and planning files are deleted': 'Ältere Logs und Planungsdateien werden gelöscht',
    'Audible stores': 'Audible-Shops', 'An ASIN is searched in these stores': 'In diesen Shops wird eine ASIN gesucht',
    'Use the Audible API': 'Audible-API verwenden', 'Off = read web pages only (not recommended)': 'Aus = nur Webseiten lesen (nicht empfohlen)',
    'Interval Audible API (s)': 'Abstand Audible-API (s)', 'Interval Audible web page (s)': 'Abstand Audible-Webseite (s)',
    'Same-book runtime tolerance (%)': 'Laufzeit-Toleranz gleiches Buch (%)',
    'Only decides whether the ASIN still names the same BOOK (another recording can be 12-16 % longer). Lower values lose correct matches. Narrator, publisher, year and cover need the runtime within 10 min anyway': 'Entscheidet nur, ob die ASIN noch dasselbe BUCH meint (eine andere Aufnahme kann 12-16 % länger sein). Kleinere Werte verlieren richtige Treffer. Sprecher, Verlag, Jahr und Cover brauchen ohnehin eine Laufzeit auf 10 min genau',
    'Same-book runtime tolerance (min)': 'Laufzeit-Toleranz gleiches Buch (min)', 'Both limits must be exceeded': 'Beide Grenzen müssen überschritten sein',
    'Use Hardcover': 'Hardcover verwenden',
    'Ratings and the print ISBN. The scan stops shortly before the daily limit': 'Bewertungen und die Print-ISBN. Der Scan stoppt kurz vor dem Tageslimit',
    'Paid Hardcover plan': 'Bezahltes Hardcover-Abo',
    'Off = free plan: at most 5000 requests per day. On = 50000 per day. Both 60 per minute': 'Aus = Free-Plan: höchstens 5000 Anfragen pro Tag. An = 50000 pro Tag. Beide 60 pro Minute',
    'Hardcover API token': 'Hardcover-API-Token', 'hardcover.app → Settings → API': 'hardcover.app → Settings → API',
    'Interval Hardcover (s)': 'Abstand Hardcover (s)', 'Hardcover allows 60 requests per minute': 'Hardcover erlaubt 60 Anfragen pro Minute',
    'Use Goodreads': 'Goodreads verwenden',
    'Experimental: Goodreads blocks bots quickly. At the first block Goodreads is stopped for the scan': 'Experimentell: Goodreads sperrt Bots schnell. Bei der ersten Sperre wird Goodreads für den Scan beendet',
    'Goodreads requests per scan': 'Goodreads-Anfragen pro Scan',
    'Remembered books do not count. Safe so far: about 25 per night': 'Gemerkte Bücher zählen nicht. Bisher sicher: etwa 25 pro Nacht',
    'Interval Goodreads (s)': 'Abstand Goodreads (s)', 'Pause after a Goodreads block (s)': 'Pause nach einer Goodreads-Sperre (s)',
    'Pauses before Goodreads is skipped': 'Pausen, bevor Goodreads übersprungen wird', '0 = stop at the first block (recommended)': '0 = bei der ersten Sperre aufhören (empfohlen)',
    'Re-check books without Goodreads (days)': 'Bücher ohne Goodreads erneut prüfen (Tage)',
    'Also load Goodreads book pages': 'Auch Goodreads-Buchseiten laden',
    'Off = read the rating from the search list only (book pages trigger the bot block much sooner)': 'Aus = Bewertung nur aus der Suchliste lesen (Buchseiten lösen die Bot-Sperre viel früher aus)',
    'Order in the rating block': 'Reihenfolge im Bewertungsblock', 'Comma separated, e.g. audible,hardcover,goodreads': 'Kommagetrennt, z.B. audible,hardcover,goodreads',
    'Set missing covers': 'Fehlende Cover setzen', 'Only for a verified ASIN': 'Nur bei verifizierter ASIN',
    'Replace existing covers': 'Vorhandene Cover ersetzen',
    'Only for a verified ASIN (same recording); the old cover is backed up and can be restored': 'Nur bei verifizierter ASIN (gleiche Aufnahme); das alte Cover wird gesichert und kann wiederhergestellt werden',
    'Genre language': 'Genre-Sprache', 'en = translate German Audible genres, keep = unchanged': 'en = deutsche Audible-Genres übersetzen, keep = unverändert',
    'Merge series automatically': 'Reihen automatisch zusammenführen', 'Only safe cases; uncertain ones go to the review list': 'Nur sichere Fälle; unsichere kommen in die Prüfliste',
    'Unify author spellings': 'Autoren-Schreibweisen vereinheitlichen', 'Keep only real authors': 'Nur echte Autoren behalten',
    'Removes illustrators, translators, editors, forewords … from the author field (only when a real author remains); never taken from Audible': 'Entfernt Illustratoren, Übersetzer, Herausgeber, Vorworte … aus dem Autorenfeld (nur wenn ein echter Autor bleibt); werden nie von Audible übernommen',
    'Remove empty authors': 'Leere Autoren entfernen',
    'Deletes only ABS authors without a single book (left-overs after merges); books are never deleted': 'Löscht nur ABS-Autoren ohne ein einziges Buch (Reste nach Zusammenführungen); Bücher werden nie gelöscht',
    'Create the library report': 'Bibliotheks-Report erstellen', 'Set lock_series after a merge': 'lock_series nach Zusammenführung setzen',
    'Old variant, not recommended': 'Alte Variante, nicht empfohlen',
    'Language': 'Sprache', 'English (default) or Deutsch': 'English (Standard) oder Deutsch',
    'Main source: ASIN, ratings, metadata, cover': 'Hauptquelle: ASIN, Bewertungen, Metadaten, Cover',
    'Ratings and print ISBN': 'Bewertungen und Print-ISBN', 'Ratings – experimental': 'Bewertungen – experimentell',
})

# --- statuses, labels, coverage rows, caches (app.py / core.py) ---
add({
    'Not found': 'Nicht gefunden', 'Error': 'Fehler', 'Other edition': 'Andere Ausgabe', 'No ratings yet': 'Noch keine Bewertungen',
    'OK': 'OK', 'Ignored': 'Ignoriert',
    'Scan': 'Scan', 'Series & report only': 'Nur Reihen & Report', 'Single book': 'Einzelnes Buch', 'Apply test run': 'Testlauf übernehmen',
    'ASIN': 'ASIN', 'Cover': 'Cover', 'Series': 'Reihe', 'Narrators': 'Sprecher', 'Description': 'Beschreibung', 'Author': 'Autor',
    'Genres': 'Genres', 'Publisher': 'Verlag', 'Year': 'Jahr', 'ISBN': 'ISBN', 'Ratings': 'Bewertungen',
    'Audible ratings': 'Audible-Bewertungen', 'Goodreads ratings': 'Goodreads-Bewertungen', 'Hardcover': 'Hardcover',
    'Hardcover IDs': 'Hardcover-IDs', 'Goodreads pages': 'Goodreads-Seiten', 'Audible series': 'Audible-Reihen',
    'Audible authors': 'Audible-Autoren', 'Rating history': 'Bewertungsverlauf',
    'unknown': 'unbekannt', 'blocked (last scan)': 'gesperrt (letzter Scan)', 'ok (last scan)': 'ok (letzter Scan)',
    'Books': 'Bücher', 'Authors': 'Autoren', 'Other': 'Sonstiges',
})

# --- flash messages and API answers ---
add({
    'Wrong password.': 'Falsches Passwort.', 'Old password is wrong.': 'Altes Passwort ist falsch.',
    'At least 6 characters and not "admin".': 'Mindestens 6 Zeichen und nicht „admin“.', 'The passwords do not match.': 'Die Passwörter stimmen nicht überein.',
    'Password changed.': 'Passwort geändert.', 'Book is ignored.': 'Buch wird ignoriert.', 'No longer ignored.': 'Wird nicht mehr ignoriert.',
    'Please enter an ASIN or a link.': 'Bitte eine ASIN oder einen Link eingeben.', 'Nothing selected.': 'Nichts ausgewählt.',
    "'%(a)s' → '%(b)s' confirmed.": "„%(a)s“ → „%(b)s“ bestätigt.", 'Merge discarded (it was only planned).': 'Zusammenführung verworfen (sie war nur geplant).',
    'ASIN %(a)s saved: it is written for this book with the next scan (and locked). Use "Test run first" on the book page to check it before.':
        'ASIN %(a)s gespeichert: sie wird beim nächsten Scan für dieses Buch geschrieben (und gesperrt). Mit „Erst als Testlauf“ auf der Buchseite vorher prüfen.',
    'Proposal confirmed: %(c)s. This kind is not written automatically yet - please change it in ABS.':
        'Vorschlag bestätigt: %(c)s. Diese Art wird noch nicht automatisch geschrieben – bitte in ABS ändern.',
    'Proposal rejected: the book stays unchanged.': 'Vorschlag verworfen: das Buch bleibt unverändert.',
    'Decision saved. It is applied with the next scan.': 'Entscheidung gespeichert. Sie wird beim nächsten Scan angewendet.',
    'The scan is being stopped.': 'Der Scan wird abgebrochen.', 'Please choose at least one task.': 'Bitte mindestens eine Aufgabe wählen.',
    'No books for this selection (%(n)s).': 'Keine Bücher für diese Auswahl (%(n)s).', '%(l)s: invalid value': '%(l)s: ungültiger Wert',
    'Unknown section.': 'Unbekannter Bereich.', '%(s)s: saved.': '%(s)s: gespeichert.',
    'A scan is already running.': 'Es läuft bereits ein Scan.', 'ABS URL and token are missing (settings).': 'ABS-URL und Token fehlen (Einstellungen).',
    'Not recognised. Expected: ASIN, Audible, Goodreads or Hardcover link.': 'Nicht erkannt. Erwartet: ASIN, Audible-, Goodreads- oder Hardcover-Link.',
    'Connected. %(n)s libraries found.': 'Verbunden. %(n)s Bibliotheken gefunden.', 'ABS answers with HTTP %(c)s': 'ABS antwortet mit HTTP %(c)s',
    'wrong token?': 'Token falsch?', 'No connection: %(e)s': 'Keine Verbindung: %(e)s', 'Connected as %(n)s.': 'Verbunden als %(n)s.',
    'Hardcover answers with HTTP %(c)s': 'Hardcover antwortet mit HTTP %(c)s', 'Notifications work.': 'Benachrichtigungen funktionieren.',
    'Test message sent.': 'Testnachricht gesendet.', 'Sending failed (check the URL).': 'Senden fehlgeschlagen (URL prüfen).',
    'Next run: %(t)s': 'Nächste Ausführung: %(t)s', 'Invalid cron expression': 'Ungültiger Cron-Ausdruck',
})

# --- templates + app.js ---
add({
    # navigation / base
    'Overview': 'Übersicht', 'Review list': 'Prüfliste', 'Status': 'Status', 'Scans': 'Scans', 'History': 'Verlauf', 'Settings': 'Einstellungen',
    'Menu': 'Menü', 'Locked books': 'Gesperrte Bücher', 'Tag': 'Tag', 'Lock tags in ABS (lock_all, lock_title, ...) stop the script from changing these fields. Counted from the last check of each book.': 'Lock-Tags in ABS (lock_all, lock_title, …) hindern das Skript daran, diese Felder zu ändern. Gezählt aus der letzten Prüfung jedes Buchs.', 'Same ASIN twice': 'Gleiche ASIN mehrfach', 'Several books in the library carry the same ASIN - usually one book twice (other format, copy). Nothing is changed automatically.': 'Mehrere Bücher der Bibliothek tragen dieselbe ASIN – meist ein Buch doppelt (anderes Format, Kopie). Es wird nichts automatisch geändert.', 'Hardcover link saved: it is used for this book from the next scan on.': 'Hardcover-Link gespeichert: er wird ab dem nächsten Scan für dieses Buch verwendet.', '%(c)s is no valid ISBN - nothing written.': '%(c)s ist keine gültige ISBN – nichts geschrieben.', 'ABS not reachable.': 'ABS nicht erreichbar.', 'Run mode': 'Lauf-Modus', 'Random books per maintenance scan': 'Zufällige Bücher pro Pflege-Scan', 'Besides new and changed books': 'Zusätzlich zu neuen und geänderten Büchern',
    'auto = build-up until every book was checked once, then maintenance. Maintenance: new and changed books always, plus a few random due books': 'auto = Aufbau, bis jedes Buch einmal geprüft ist, dann Pflege. Pflege: neue und geänderte Bücher immer, dazu einige zufällige fällige',
    'Lock tag for fields you changed in ABS': 'Lock-Tag für Felder, die du in ABS geändert hast', 'Fields you change yourself in ABS are never overwritten anyway; this also sets the lock_<field> tag in ABS': 'Felder, die du selbst in ABS änderst, werden ohnehin nie überschrieben; das setzt zusätzlich den lock_<Feld>-Tag in ABS',
    'Changed by you in ABS': 'Von dir in ABS geändert', 'After the script wrote these fields you (or another tool) changed them in ABS. They are respected and never overwritten again.': 'Nachdem das Skript diese Felder geschrieben hatte, hast du (oder ein anderes Werkzeug) sie in ABS geändert. Sie werden respektiert und nie wieder überschrieben.', 'Remove ratings': 'Bewertungen entfernen', 'instead of a normal scan': 'statt eines normalen Scans', 'all books': 'alle Bücher',
    'Removes the chosen lines from the rating block of all books (or the list under "Which books"). Every change can be undone in the History. The source is paused for these books until the refresh time; switch it off in the settings to keep it away for good.':
        'Entfernt die gewählten Zeilen aus dem Bewertungsblock aller Bücher (oder der Liste unter „Welche Bücher“). Jede Änderung lässt sich im Verlauf rückgängig machen. Die Quelle wird für diese Bücher bis zur Auffrischzeit pausiert; damit sie dauerhaft weg bleibt, in den Einstellungen abschalten.',
    'Please choose at least one source.': 'Bitte mindestens eine Quelle wählen.', 'Remove the chosen ratings from this book? (can be undone in the History)': 'Die gewählten Bewertungen von diesem Buch entfernen? (im Verlauf rückgängig machbar)', 'Remove': 'Entfernen', 'ABS backup before a real scan': 'ABS-Backup vor echtem Scan', 'Ask ABS for a backup before anything is written (takes a few minutes for big libraries)': 'ABS legt ein Backup an, bevor etwas geschrieben wird (dauert bei großen Bibliotheken einige Minuten)', 'yes': 'ja', 'no': 'nein', 'Scan running': 'Scan läuft', 'Search…': 'Suchen…', 'running': 'läuft', 'next scan': 'nächster Scan', 'Light / dark': 'Hell / dunkel',
    'Password': 'Passwort', 'Log out': 'Abmelden',
    # overview
    'Not set up yet: please enter the ABS URL, token and libraries under': 'Noch nicht eingerichtet: bitte ABS-URL, Token und Bibliotheken eintragen unter',
    'To review': 'Zu prüfen', '%(a)s books · %(b)s uncertain series/authors · %(c)s merges to confirm': '%(a)s Bücher · %(b)s unsichere Reihen/Autoren · %(c)s Zusammenführungen zum Bestätigen',
    'Open the review list': 'Zur Prüfliste', 'Last scan': 'Letzter Scan', 'Test run': 'Testlauf', 'no scan yet': 'noch kein Scan',
    'Next scan': 'Nächster Scan', 'no schedule': 'kein Zeitplan', 'off': 'aus', 'on': 'an', 'Start a scan': 'Scan starten',
    'Library': 'Bibliothek', 'books checked': 'Bücher geprüft', 'with ratings': 'mit Bewertungen', 'not found': 'nicht gefunden',
    'other edition': 'andere Ausgabe', 'no ratings yet': 'noch keine Bewertungen', 'Coverage of all fields': 'Abdeckung aller Felder',
    'Library report': 'Bibliotheks-Report', 'Books without ratings': 'Bücher ohne Bewertungen', 'all': 'alle', 'Title or author': 'Titel oder Autor',
    'Filter': 'Filtern', 'Book': 'Buch', 'Reason': 'Grund', 'from a test run': 'aus einem Testlauf', 'None.': 'Keine.',
    'The list fills after the first scan.': 'Die Liste füllt sich nach dem ersten Scan.', 'First 300 of %(n)s - please filter.': 'Erste 300 von %(n)s – bitte filtern.',
    # scans
    'Cancel': 'Abbrechen', 'Metadata & ratings': 'Metadaten & Bewertungen', 'Normalize authors': 'Autoren vereinheitlichen', 'Merge series': 'Reihen zusammenführen',
    'Remove illustrators, translators, editors, forewords … from the author field (only when a real author remains)': 'Illustratoren, Übersetzer, Herausgeber, Vorwort … aus dem Autorenfeld entfernen (nur wenn ein echter Autor bleibt)',
    'Only real authors': 'Nur echte Autoren', 'Delete ABS authors without a single book': 'ABS-Autoren ohne ein einziges Buch löschen',
    'Test run (write nothing)': 'Testlauf (nichts schreiben)', 'Start scan': 'Scan starten',
    'A test run only plans; afterwards you can review the planned changes and apply them selectively. Series and authors are always checked across the whole library.':
        'Ein Testlauf plant nur; die geplanten Änderungen kannst du danach prüfen und gezielt übernehmen. Reihen und Autoren werden immer über die ganze Bibliothek geprüft.',
    'Options for this scan only': 'Optionen nur für diesen Scan', 'does not change saved settings': 'ändert keine gespeicherten Einstellungen',
    '0 = all books are due': '0 = alle Bücher sind fällig', 'experimental': 'experimentell', 'Which books': 'Welche Bücher',
    'for metadata & ratings': 'für Metadaten & Bewertungen', 'all due': 'alle fälligen', 'problems only': 'nur Probleme',
    'new only (never checked)': 'nur neue (nie geprüft)', 'own list': 'eigene Liste',
    'One book per line: title (as in the book list), ABS item id or key': 'Ein Buch pro Zeile: Titel (wie in der Bücherliste), ABS-Item-ID oder Schlüssel',
    'Scheduled scans use the saved settings. Next scheduled scan:': 'Geplante Scans nutzen die gespeicherten Einstellungen. Nächster geplanter Scan:',
    'Start': 'Start', 'Kind': 'Art', 'Result': 'Ergebnis', 'scheduled': 'geplant', 'manual': 'manuell', 'Details': 'Details', 'No scans yet.': 'Noch keine Scans.',
    'Real scan: the changes are written to ABS (ABS backup first). Continue?': 'Echter Scan: die Änderungen werden in ABS geschrieben (vorher ABS-Backup). Fortfahren?',
    'Start real scan': 'Echten Scan starten',
    # review list
    '%(n)s more': '%(n)s weitere', 'Volumes': 'Bände', 'Sections': 'Bereiche',
    'Only what needs a decision from you, plus the automatically merged series and authors to confirm or undo. Nothing uncertain is written without you.':
        'Hier steht nur, was eine Entscheidung von dir braucht, plus die automatisch zusammengeführten Reihen und Autoren zum Bestätigen oder Zurücknehmen. Nichts Unsicheres wird ohne dich geschrieben.',
    'New ASIN, title suggestion or Hardcover match where the evidence is not enough for an automatic change. "Apply" writes the suggestion (can be undone in the history), "Discard" leaves the book as it is - the suggestion does not come back.':
        'Neue ASIN, Titelvorschlag oder Hardcover-Zuordnung, bei denen die Belege für eine automatische Änderung nicht reichen. „Übernehmen“ schreibt den Vorschlag (danach im Verlauf rückgängig machbar), „Verwerfen“ lässt das Buch wie es ist – der Vorschlag kommt nicht wieder.',
    'Title': 'Titel', 'Apply': 'Übernehmen', 'Discard': 'Verwerfen', 'Suggestion': 'Vorschlag', 'Evidence': 'Belege', 'current': 'aktuell',
    'Suggestion on Audible': 'Vorschlag auf Audible', 'No open books.': 'Keine offenen Bücher.',
    'Series are merged automatically only when the names are equal after cleaning (or almost equal and the volume numbers complement or interleave), every book shares an author, the edition is the same and no numbers overlap.':
        'Zusammengeführt wird automatisch nur, wenn die Namen nach dem Bereinigen gleich sind (oder fast gleich und sich die Bandnummern ergänzen bzw. verzahnen), jedes Buch einen gemeinsamen Autor hat, die Ausgabe gleich ist und sich keine Nummern überschneiden.',
    'Authors are normalized automatically only with evidence for the same person: spelling (initials, spaces, accents), the same Audible author id or identical books. The goal is one spelling in Latin script.':
        'Vereinheitlicht wird automatisch nur bei Beleg für dieselbe Person: Schreibweise (Initialen, Leerzeichen, Akzente), dieselbe Audible-Autor-ID oder identische Bücher. Ziel ist eine Schreibweise in lateinischer Schrift.',
    'Uncertain - your decision': 'Unsicher – deine Entscheidung', 'Candidate': 'Kandidat', 'Target': 'Ziel', 'Why uncertain': 'Warum unsicher',
    'Merge': 'Zusammenführen', 'Keep separate': 'Getrennt lassen', 'No uncertain cases.': 'Keine unsicheren Fälle.',
    'Merged automatically': 'Automatisch zusammengeführt', 'before': 'vorher', 'now': 'jetzt', 'applied': 'übernommen',
    'planned (test run)': 'geplant (Testlauf)', '%(n)s book(s)': '%(n)s Buch/Bücher', 'same name after cleaning': 'gleicher Name nach Bereinigung',
    'almost the same name, numbers complement': 'fast gleicher Name, Nummern ergänzen sich', 'same person, different spelling': 'gleiche Person, andere Schreibweise',
    'Correct': 'Passt', 'Undo': 'Rückgängig',
    'Undo this merge? The old names are restored and these will not be merged again.': 'Zusammenführung rückgängig machen? Die alten Namen werden wiederhergestellt und hier wird künftig nicht mehr zusammengeführt.',
    'Nothing open.': 'Nichts offen.', 'As of': 'Stand',
    'The same recording twice in the library (same author, same narrator, runtime equal to 2 seconds). The script changes nothing here - clean up in ABS itself.':
        'Gleiche Aufnahme zweimal in der Bibliothek (gleicher Autor, gleicher Sprecher, Laufzeit auf 2 Sekunden gleich). Das Skript ändert hier nichts – in ABS selbst aufräumen.',
    'No duplicates found.': 'Keine Doppelungen gefunden.',
    # logs
    'All as ZIP': 'Alle als ZIP', 'for troubleshooting': 'für Fehlersuche', 'File': 'Datei', 'Modified': 'Geändert', 'Size': 'Größe', 'No logs yet.': 'Noch keine Logs.',
    'Filter (e.g. WARNING, Rejected, title)': 'Filter (z.B. WARNING, Rejected, Titel)', 'follow live': 'live mitlaufen', 'Download': 'Herunterladen',
    'showing the last 400 KB': 'es werden die letzten 400 KB angezeigt',
    # books
    'Search title or author': 'Titel oder Autor suchen', 'Sort': 'Sortierung', 'by certainty': 'nach Sicherheit', 'by title': 'nach Titel',
    'by author': 'nach Autor', 'by status': 'nach Status', 'Direction': 'Richtung', 'ascending': 'aufsteigend', 'descending': 'absteigend',
    'Certainty ≤': 'Sicherheit ≤', 'Show': 'Anzeigen',
    'The certainty (0-100) says how sure the script is that the Audible product behind the ASIN is exactly this book: title 25, author 20, runtime 20, narrator 12, series/volume number 10, publisher+year 8, Hardcover confirmation 5; deductions for ASIN search (-5), other edition (-20), open review entry (-15). Check low values first.':
        'Die Sicherheit (0–100) sagt, wie sicher das Skript ist, dass das Audible-Produkt hinter der ASIN genau dieses Buch ist: Titel 25, Autor 20, Laufzeit 20, Sprecher 12, Reihen-/Bandnummer 10, Verlag+Jahr 8, Hardcover-Bestätigung 5; Abzüge für ASIN-Suche (−5), andere Ausgabe (−20), offenen Prüfeintrag (−15). Niedrige Werte zuerst prüfen.',
    'Certainty': 'Sicherheit', 'No results yet.': 'Noch keine Ergebnisse.', 'First 1000 of %(n)s shown - please filter.': 'Erste 1000 von %(n)s angezeigt – bitte filtern.',
    # settings
    'Library ids, comma separated (the list appears after a successful connection)': 'Bibliotheks-IDs, kommagetrennt (Liste erscheint nach erfolgreicher Verbindung)',
    '•••••••• saved - leave empty = unchanged': '•••••••• gespeichert – leer lassen = unverändert', 'not set': 'nicht gesetzt',
    'Test connection': 'Verbindung testen', 'Test': 'Testen', 'Advanced': 'Erweitert', 'View and download logs': 'Logs ansehen und herunterladen',
    'Save %(s)s': '%(s)s speichern', 'Onboarding': 'Einführung',
    'The tour explains the pages of the app in a few steps. It starts automatically on the first visit.': 'Der Rundgang erklärt in wenigen Schritten die Seiten der App. Er startet beim ersten Besuch automatisch.',
    'Start the tour': 'Einführungstour starten',
    'Switch between light and dark at the top right with ☾ / ☀ (on the first visit the system setting applies, afterwards your choice in this browser).':
        'Hell oder dunkel wechselst du oben rechts mit ☾ / ☀ (beim ersten Besuch gilt die Systemeinstellung, danach deine Wahl in diesem Browser).',
    'Schedule active': 'Zeitplan aktiv', 'Cron mode': 'Cron-Modus', 'Off = weekdays + time': 'Aus = Wochentage + Uhrzeit', 'Weekdays': 'Wochentage',
    'Mon': 'Mo', 'Tue': 'Di', 'Wed': 'Mi', 'Thu': 'Do', 'Fri': 'Fr', 'Sat': 'Sa', 'Sun': 'So', 'Time': 'Uhrzeit', 'Cron expression': 'Cron-Ausdruck',
    'Check': 'Prüfen', 'minute hour day month weekday, e.g.': 'Minute Stunde Tag Monat Wochentag, z.B.', 'daily at 02:30': 'täglich 02:30',
    'Scheduled scans use the "Test run" setting under Run.': 'Geplante Scans verwenden die Einstellung „Testlauf“ unter Lauf.',
    'Apprise URL': 'Apprise-URL', 'e.g. ntfy://ntfy.sh/my-topic or discord://webhook_id/token': 'z.B. ntfy://ntfy.sh/mein-topic oder discord://webhook_id/token',
    'Empty = off. Supports ntfy, Discord, Telegram, Gotify, e-mail and many more': 'Leer = aus. Unterstützt ntfy, Discord, Telegram, Gotify, E-Mail u.v.m.',
    'formats': 'Formate', 'Notify on': 'Benachrichtigen bei', 'errors only': 'nur Fehlern', 'every scan': 'jedem Scan',
    # book page
    'title': 'Titel', 'author': 'Autor', 'runtime': 'Laufzeit', 'narrator': 'Sprecher', 'series/volume': 'Reihe/Band', 'publisher+year': 'Verlag+Jahr',
    'found by search': 'per Suche gefunden', 'open review entry': 'offener Prüfeintrag', 'Subtitle': 'Untertitel', 'Runtime': 'Laufzeit', 'Narrator': 'Sprecher',
    'Open in ABS': 'In ABS öffnen', 'checked': 'geprüft', 'test run': 'Testlauf', 'Manual overrides': 'Manuelle Vorgaben', 'Reasoning': 'Begründung',
    'made up of': 'so setzt sie sich zusammen', 'Source of the ASIN': 'Quelle der ASIN', 'Hardcover found via': 'Hardcover gefunden über',
    'Evidence of the ASIN search': 'Belege der ASIN-Suche', 'Comparison': 'Vergleich', 'Field': 'Feld', 'Audible only': 'nur Audible',
    '"≠" does not mean wrong: ABS values are only filled when they are empty (exceptions: broken file-name titles, cover for a verified ASIN).':
        '„≠“ heißt nicht falsch: ABS-Werte werden nur gefüllt, wenn sie leer sind (Ausnahmen: kaputte Dateinamen-Titel, Cover bei verifizierter ASIN).',
    'Changes to this book': 'Änderungen an diesem Buch', 'Old': 'Alt', 'New': 'Neu', '%(n)s characters': '%(n)s Zeichen', 'Image': 'Bild',
    'undone': 'rückgängig', 'written': 'geschrieben', 'No changes recorded.': 'Keine Änderungen protokolliert.', 'Repair manually': 'Manuell reparieren',
    'ASIN or link': 'ASIN oder Link', 'ASIN, Audible, Goodreads or Hardcover link (detected automatically)': 'ASIN, Audible-, Goodreads- oder Hardcover-Link (wird automatisch erkannt)',
    'or separate fields': 'oder getrennte Felder', 'Audible ASIN / link': 'Audible-ASIN / Link', 'Goodreads link': 'Goodreads-Link', 'Hardcover link': 'Hardcover-Link',
    'Apply & process': 'Übernehmen & bearbeiten', 'Test run first': 'Erst als Testlauf', 'Check again without override': 'Ohne Vorgabe neu prüfen',
    'Stop ignoring': 'Nicht mehr ignorieren', 'Ignore': 'Ignorieren',
    'A manual ASIN is written to ABS and never replaced automatically afterwards. A Goodreads or Hardcover link is used permanently for this book.':
        'Eine manuelle ASIN wird in ABS geschrieben und danach nie automatisch ersetzt. Ein Goodreads- oder Hardcover-Link wird fest für dieses Buch verwendet.',
    'Log of the last check': 'Log der letzten Prüfung', 'no log': 'kein Log',
    # history
    'Abridged': 'Gekürzt', 'Empty author': 'Leerer Autor',
    'All changes really written to ABS (test runs do not appear here, they are listed under': 'Alle Änderungen, die wirklich in ABS geschrieben wurden (Testläufe erscheinen hier nicht, die stehen unter',
    'Every change can be undone on its own, per book or for the whole scan; replaced covers are restored from the backup.':
        'Jede Änderung lässt sich einzeln, pro Buch oder für den ganzen Scan rückgängig machen; ersetzte Cover werden aus der Sicherung wiederhergestellt.',
    'Changes': 'Änderungen', 'Fields': 'Felder', 'Undone': 'Rückgängig', 'Nothing written to ABS yet.': 'Noch nichts in ABS geschrieben.',
    'Filter books': 'Buch filtern', 'all fields': 'alle Felder',
    'Undo the WHOLE scan? All %(n)s still active changes are reset in ABS.': 'Den GANZEN Scan rückgängig machen? Alle %(n)s noch aktiven Änderungen werden in ABS zurückgesetzt.',
    'whole scan': 'ganzer Scan', 'Undo whole scan': 'Ganzen Scan rückgängig', 'available without filter': 'ohne Filter verfügbar',
    'backup available': 'Sicherung vorhanden', 'no cover': 'kein Cover', 'cannot be undone': 'nicht rückgängig machbar', 'Undo this change?': 'Diese Änderung rückgängig machen?',
    'only this field': 'nur dieses Feld', 'Undo all changes of this book in this scan?': 'Alle Änderungen dieses Buches in diesem Scan rückgängig machen?',
    'whole book': 'ganzes Buch', 'all fields of this book': 'alle Felder dieses Buches', 'No changes': 'Keine Änderungen', 'for this filter': 'für diesen Filter',
    # run page
    'back to the book': 'zurück zum Buch', 'End': 'Ende', 'Download log': 'Log herunterladen', 'Apply this test run': 'Diesen Testlauf übernehmen',
    'Review and select changes': 'Änderungen prüfen und auswählen', 'grouped, risky ones first, with checkboxes': 'gruppiert, riskante zuerst, mit Häkchen',
    'Writes exactly the %(n)s planned changes to ABS - but only where the ABS value is still the same as in the test run. If a book was changed in the meantime (by you or another scan) or locked, that change is skipped and named in the log. An ABS backup is made first (if enabled); everything then appears under History and can be undone.':
        'Schreibt genau die %(n)s geplanten Änderungen in ABS – aber nur dort, wo der ABS-Wert noch derselbe ist wie beim Testlauf. Wurde ein Buch inzwischen geändert (von dir oder einem anderen Scan) oder gesperrt, wird diese Änderung übersprungen und im Log genannt. Vorher wird ein ABS-Backup angelegt (falls eingestellt); alles erscheint danach unter Verlauf und lässt sich rückgängig machen.',
    'Check first (write nothing)': 'Erst prüfen (nichts schreiben)', 'Really write %(n)s changes to ABS now?': '%(n)s Änderungen jetzt wirklich in ABS schreiben?',
    'Planned changes': 'Geplante Änderungen', 'Changes made': 'Durchgeführte Änderungen', 'Before': 'Vorher', 'After': 'Nachher', 'No changes.': 'Keine Änderungen.',
    '%(n)s more in the log / planned_changes.': '%(n)s weitere im Log / planned_changes.',
    'Starting …': 'Startet …', 'Time left': 'Restzeit', 'Audible found': 'Audible gefunden', 'without Audible data': 'ohne Audible-Daten',
    'blocked': 'gesperrt', 'new ASIN': 'neue ASIN',
    # run review
    'Review test run': 'Testlauf prüfen', 'risky change: check carefully': 'riskante Änderung: genau prüfen', 'back to the test run': 'zum Testlauf',
    '%(n)s changes': '%(n)s Änderungen', 'Clear filter': 'Filter aufheben',
    'Checked changes are written - but only if the ABS value is still the same as in the test run (otherwise skipped and named in the log). No new Audible requests. ABS backup first, afterwards everything can be undone under':
        'Angehakte Änderungen werden geschrieben – aber nur, wenn der ABS-Wert noch derselbe ist wie beim Testlauf (sonst übersprungen und im Log genannt). Keine neuen Audible-Anfragen. Vorher ABS-Backup, danach alles rückgängig machbar unter',
    'Write the selected changes to ABS now?': 'Die ausgewählten Änderungen jetzt in ABS schreiben?', 'Write': 'Schreiben', 'select all': 'alle wählen', 'none': 'keine',
    'Check selection (write nothing)': 'Auswahl prüfen (nichts schreiben)', 'Apply selection': 'Auswahl übernehmen', 'Check risky changes first': 'Riskante Änderungen zuerst prüfen',
    'ASIN change or new ASIN, title, language, new series, authors, ISBN replacement: they change which book is meant or what it is called. Look at each one (a click on the title shows comparison and reason).':
        'ASIN-Wechsel oder neue ASIN, Titel, Sprache, neue Reihe, Autoren, ISBN-Ersatz: Sie ändern, welches Buch gemeint ist oder wie es heißt. Jede einzeln ansehen (Klick auf den Titel zeigt Vergleich und Begründung).',
    'whole group': 'ganze Gruppe', '%(a)s of %(b)s selected': '%(a)s von %(b)s ausgewählt',
    # login / password
    'Set password': 'Passwort festlegen', 'Change password': 'Passwort ändern', 'Please choose your own password before first use.': 'Bitte vergib vor dem ersten Benutzen ein eigenes Passwort.',
    'Old password': 'Altes Passwort', 'New password (min. 6 characters)': 'Neues Passwort (min. 6 Zeichen)', 'Repeat': 'Wiederholen', 'Save': 'Speichern',
    'Log in': 'Anmelden', 'First password:': 'Erstes Passwort:', 'changed afterwards': 'wird danach geändert',
    # status
    'Hardcover requests left (account)': 'Hardcover-Anfragen übrig (Account)', 'Own budget: %(n)s of %(m)s used today (UTC)': 'Eigenes Budget: %(n)s von %(m)s heute verbraucht (UTC)', 'as of': 'Stand', 'Reset': 'Reset',
    'daily (Hardcover does not report the time)': 'täglich (Uhrzeit meldet Hardcover nicht)', 'Entries in the review list': 'Einträge in der Prüfliste',
    'The Hardcover daily limit resets every day; the scan stops automatically shortly before it (reserve 25). Goodreads is stopped at the first block and tried again in the next scan.':
        'Das Hardcover-Tageslimit setzt sich täglich zurück; der Scan stoppt automatisch kurz davor (Reserve 25). Goodreads wird bei der ersten Sperre sofort beendet und beim nächsten Scan neu versucht.',
    'Library coverage': 'Abdeckung der Bibliothek', 'present': 'vorhanden', 'Share': 'Anteil', 'Trend': 'Verlauf',
    'counted from the next scan on': 'wird ab dem nächsten Scan gezählt', '%(n)s without': '%(n)s ohne',
    'No library report yet (made by the series task).': 'Noch kein Bibliotheks-Report vorhanden (entsteht beim Reihen-Task).',
    'trend since %(t)s (%(n)s data points)': 'Verlauf seit %(t)s (%(n)s Messpunkte)', 'the trend appears from the second report on': 'Verlauf erscheint ab dem zweiten Bericht',
    'ISBN field wrong, no replacement found': 'ISBN-Feld fehlerhaft, kein Ersatz gefunden',
    'The ISBN field holds e.g. an ASIN. It is only replaced with a verified ISBN (print edition via Hardcover, otherwise the audiobook ISBN from Audible); if there is none, the value stays. Nothing to decide - the next scan tries again':
        'Im ISBN-Feld steht z.B. eine ASIN. Ersetzt wird nur mit einer geprüften ISBN (Print-Ausgabe über Hardcover, sonst die Hörbuch-ISBN von Audible); gibt es keine, bleibt der Wert stehen. Nichts zu entscheiden – der nächste Scan versucht es wieder',
    'Hardcover was not available in the last scan': 'Hardcover war im letzten Scan nicht verfügbar', 'Show %(n)s books': '%(n)s Bücher anzeigen',
    'ISBN field': 'ISBN-Feld', 'Caches': 'Zwischenspeicher', '%(n)s entries': '%(n)s Einträge',
    # app.js: theme, tour, dialog
    'Switch to dark': 'Zu dunkel wechseln', 'Switch to light': 'Zu hell wechseln', 'Welcome to ABS Ratings': 'Willkommen bei ABS Ratings',
    'A short tour in 9 steps. You can skip it at any time and restart it under Settings.': 'Ein kurzer Rundgang in 9 Schritten. Du kannst ihn jederzeit überspringen und unter Einstellungen neu starten.',
    'What needs you, the last and the next scan, and the state of the library.': 'Was dich braucht, der letzte und der nächste Scan und der Stand der Bibliothek.',
    'All checked books, sorted by how certain the match is. With search, ABS ↔ Audible comparison and the reasoning of every decision.':
        'Alle geprüften Bücher, sortiert danach, wie sicher die Zuordnung ist. Mit Suche, Vergleich ABS ↔ Audible und der Begründung jeder Entscheidung.',
    'Everything that needs your decision: uncertain book matches, series and authors (left menu), plus merged series/authors to confirm or undo.':
        'Alles, was deine Entscheidung braucht: unsichere Buch-Zuordnungen, Reihen und Autoren (linkes Menü), dazu zusammengeführte Reihen/Autoren zum Bestätigen oder Zurücknehmen.',
    'Coverage of every metadata field and the state of the sources (Audible, Hardcover, Goodreads).': 'Abdeckung jedes Metadatenfelds und der Zustand der Quellen (Audible, Hardcover, Goodreads).',
    'Start a scan here: metadata & ratings, authors, series - each a checkbox. Start with a test run: nothing is written and you check the changes first.':
        'Hier startest du einen Scan: Metadaten & Bewertungen, Autoren, Reihen – jeweils ein Häkchen. Beginne mit einem Testlauf: es wird nichts geschrieben und du prüfst die Änderungen zuerst.',
    'Everything that was really written to ABS - per scan, book and field. Every change can be undone on its own, per book or for the whole scan.':
        'Alles, was wirklich in ABS geschrieben wurde – pro Scan, Buch und Feld. Jede Änderung lässt sich einzeln, pro Buch oder für den ganzen Scan rückgängig machen.',
    'ABS connection, schedule, sources, notifications, logs and the language. You can restart this tour here.':
        'ABS-Verbindung, Zeitplan, Quellen, Benachrichtigungen, Logs und die Sprache. Hier kannst du diesen Rundgang neu starten.',
    'Design': 'Design', 'Light or dark. On the first visit the system setting is used, then the browser remembers your choice.':
        'Hell oder dunkel. Beim ersten Besuch gilt die Systemeinstellung, danach merkt sich der Browser deine Wahl.',
    'Skip': 'Überspringen', 'Back': 'Zurück', 'Done': 'Fertig', 'Next': 'Weiter', 'Please confirm': 'Bitte bestätigen',
})
