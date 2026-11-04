"""UI language: the web UI is written in English (default); DE holds the German translation of every UI string.
Templates use {{ _('English text') }} (with %(name)s placeholders), Python uses i18n.t(). A string without a German entry
is shown in English (never an error)."""

DE = {}


def t(text, lang='en', **kw):
    s = DE.get(text, text) if lang == 'de' else text
    return s % kw if kw else s


def add(pairs):
    DE.update(pairs)


from webui import i18n_de  # noqa: E402,F401  (fills DE)
