"""Line icons for the web UI: lucide (https://lucide.dev, ISC licence, v0.454.0), one size and stroke everywhere.
Use in templates: {{ icon("download") }} - the icon takes the colour of the text around it."""
from markupsafe import Markup

PATHS = {
    'arrow-left-right': '<path d="M8 3 4 7l4 4"/> <path d="M4 7h16"/> <path d="m16 21 4-4-4-4"/> <path d="M20 17H4"/>',
    'arrow-left': '<path d="m12 19-7-7 7-7"/> <path d="M19 12H5"/>',
    'arrow-right': '<path d="M5 12h14"/> <path d="m12 5 7 7-7 7"/>',
    'arrow-up-right': '<path d="M7 7h10v10"/> <path d="M7 17 17 7"/>',
    'book-open': '<path d="M12 7v14"/> <path d="M3 18a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h5a4 4 0 0 1 4 4 4 4 0 0 1 4-4h5a1 1 0 0 1 1 1v13a1 1 0 0 1-1 1h-6a3 3 0 0 0-3 3 3 3 0 0 0-3-3z"/>',
    'book': '<path d="M4 19.5v-15A2.5 2.5 0 0 1 6.5 2H19a1 1 0 0 1 1 1v18a1 1 0 0 1-1 1H6.5a1 1 0 0 1 0-5H20"/>',
    'check': '<path d="M20 6 9 17l-5-5"/>',
    'chevron-down': '<path d="m6 9 6 6 6-6"/>',
    'chevron-up': '<path d="m18 15-6-6-6 6"/>',
    'circle-check': '<circle cx="12" cy="12" r="10"/> <path d="m9 12 2 2 4-4"/>',
    'circle-x': '<circle cx="12" cy="12" r="10"/> <path d="m15 9-6 6"/> <path d="m9 9 6 6"/>',
    'download': '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/> <polyline points="7 10 12 15 17 10"/> <line x1="12" x2="12" y1="15" y2="3"/>',
    'headphones': '<path d="M3 14h3a2 2 0 0 1 2 2v3a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-7a9 9 0 0 1 18 0v7a2 2 0 0 1-2 2h-1a2 2 0 0 1-2-2v-3a2 2 0 0 1 2-2h3"/>',
    'log-out': '<path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/> <polyline points="16 17 21 12 16 7"/> <line x1="21" x2="9" y1="12" y2="12"/>',
    'menu': '<line x1="4" x2="20" y1="12" y2="12"/> <line x1="4" x2="20" y1="6" y2="6"/> <line x1="4" x2="20" y1="18" y2="18"/>',
    'moon': '<path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z"/>',
    'play': '<polygon points="6 3 20 12 6 21 6 3"/>',
    'search': '<circle cx="11" cy="11" r="8"/> <path d="m21 21-4.3-4.3"/>',
    'square': '<rect width="18" height="18" x="3" y="3" rx="2"/>',
    'sun': '<circle cx="12" cy="12" r="4"/> <path d="M12 2v2"/> <path d="M12 20v2"/> <path d="m4.93 4.93 1.41 1.41"/> <path d="m17.66 17.66 1.41 1.41"/> <path d="M2 12h2"/> <path d="M20 12h2"/> <path d="m6.34 17.66-1.41 1.41"/> <path d="m19.07 4.93-1.41 1.41"/>',
    'triangle-alert': '<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/> <path d="M12 9v4"/> <path d="M12 17h.01"/>',
    'undo-2': '<path d="M9 14 4 9l5-5"/> <path d="M4 9h10.5a5.5 5.5 0 0 1 5.5 5.5a5.5 5.5 0 0 1-5.5 5.5H11"/>',
    'upload': '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/> <polyline points="17 8 12 3 7 8"/> <line x1="12" x2="12" y1="3" y2="15"/>',
    'x': '<path d="M18 6 6 18"/> <path d="m6 6 12 12"/>',
}


def icon(name, size=16, label=None):
    """Inline SVG; decorative unless a label is given (then it is announced to screen readers)."""
    aria = f' role="img" aria-label="{label}"' if label else ' aria-hidden="true"'
    return Markup(f'<svg class="ico" width="{size}" height="{size}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" '
                  f'stroke-linecap="round" stroke-linejoin="round"{aria}>{PATHS.get(name, "")}</svg>')


def moon(pct, size=14, label=None):
    """Signature glyph: a flat moon phase, lit part = pct (0-100). Used for certainty."""
    try:
        p = max(0.0, min(1.0, float(pct) / 100))
    except (TypeError, ValueError):
        return Markup('')
    r, c = 6, 7
    lit = ''
    if p >= 0.995:
        lit = f'<circle cx="{c}" cy="{c}" r="{r}" fill="currentColor"/>'
    elif p > 0.005:
        rx = round(r * abs(1 - 2 * p), 2)
        sweep = 0 if p < 0.5 else 1
        lit = (f'<path d="M{c} {c - r} A{r} {r} 0 0 1 {c} {c + r} A{rx} {r} 0 0 {sweep} {c} {c - r}Z" fill="currentColor"/>')
    aria = f' role="img" aria-label="{label}"' if label else ' aria-hidden="true"'
    return Markup(f'<svg class="moon" width="{size}" height="{size}" viewBox="0 0 14 14"{aria}>'
                  f'<circle cx="{c}" cy="{c}" r="{r - 0.5}" fill="none" stroke="currentColor" stroke-opacity=".35"/>{lit}</svg>')
