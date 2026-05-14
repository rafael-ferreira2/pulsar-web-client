/* ═══════════════════════════════════════════════════════════
   GCD Streaming Client — shared icon library
   Tiny SVG sprite-style helper. Usage: icon('bolt'), icon('play', 14)
   Strokes use currentColor so they inherit text color.
   ═══════════════════════════════════════════════════════════ */
window.ICONS = {
  // Brand
  bolt:       '<path d="M13 2 4 14h7l-1 8 10-12h-7l1-8z" fill="currentColor"/>',

  // Buses
  //   pulsar — filled lightning bolt (on-brand for Apache Pulsar).
  //   kafka  — Apache Kafka style: three nodes (top, bottom, right) with a left
  //            spine and two diagonals → forms a stylized "K" of the logomark.
  pulsar:     '<path d="M13 2 4 14h7l-1 8 10-12h-7l1-8z" fill="currentColor"/>',
  kafka:      '<circle cx="6"  cy="5"  r="2"   fill="currentColor"/><circle cx="6"  cy="19" r="2"   fill="currentColor"/><circle cx="17" cy="12" r="2.2" fill="currentColor"/><path d="M6 7v10M7.5 6 15.5 11M7.5 18 15.5 13"/>',

  // Navigation / actions
  broadcast:  '<path d="M5 12a7 7 0 0 1 14 0M2.5 12a9.5 9.5 0 0 1 19 0"/><circle cx="12" cy="12" r="2" fill="currentColor"/>',
  compare:    '<path d="M7 4 3 8l4 4M17 12l4 4-4 4M3 8h12M21 16H9"/>',
  play:       '<path d="M6 4v16l14-8z" fill="currentColor"/>',
  stop:       '<rect x="6" y="6" width="12" height="12" rx="2" fill="currentColor"/>',
  pause:      '<rect x="6" y="5" width="4" height="14" fill="currentColor"/><rect x="14" y="5" width="4" height="14" fill="currentColor"/>',
  reset:      '<path d="M3 12a9 9 0 1 0 3-6.7L3 8"/><path d="M3 3v5h5"/>',
  refresh:    '<path d="M3 12a9 9 0 1 0 3-6.7L3 8"/><path d="M3 3v5h5"/>',
  download:   '<path d="M12 3v13M7 11l5 5 5-5M5 21h14"/>',
  upload:     '<path d="M12 21V8M7 13l5-5 5 5M5 3h14"/>',
  send:       '<path d="m3 11 18-8-8 18-2-8-8-2z"/>',
  trash:      '<path d="M4 7h16M9 7V4h6v3M6 7l1 14h10l1-14"/>',
  copy:       '<rect x="8" y="8" width="12" height="12" rx="2"/><path d="M16 8V5a1 1 0 0 0-1-1H5a1 1 0 0 0-1 1v10a1 1 0 0 0 1 1h3"/>',
  filter:     '<path d="M3 5h18l-7 8v7l-4-2v-5z"/>',
  search:     '<circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/>',
  plug:       '<path d="M9 2v6M15 2v6M6 8h12v3a6 6 0 0 1-12 0z"/><path d="M12 14v8"/>',
  chevron:    '<path d="m6 9 6 6 6-6"/>',
  chevronUp:  '<path d="m6 15 6-6 6 6"/>',
  arrowRight: '<path d="M5 12h14M13 6l6 6-6 6"/>',
  arrowLeft:  '<path d="M19 12H5M11 6l-6 6 6 6"/>',
  check:      '<path d="m5 12 5 5 9-11"/>',
  x:          '<path d="M6 6l12 12M6 18l12-12"/>',
  warning:    '<path d="M12 3 2 20h20zM12 10v5M12 18v.5" stroke-linecap="round"/>',
  info:       '<circle cx="12" cy="12" r="9"/><path d="M12 8v.5M12 11v6"/>',

  // Domain
  topic:      '<path d="M3 7h18M3 12h12M3 17h18"/>',
  server:     '<rect x="3" y="4" width="18" height="7" rx="2"/><rect x="3" y="13" width="18" height="7" rx="2"/><circle cx="7" cy="7.5" r="1" fill="currentColor"/><circle cx="7" cy="16.5" r="1" fill="currentColor"/>',
  cluster:    '<circle cx="6" cy="6" r="3"/><circle cx="18" cy="6" r="3"/><circle cx="12" cy="18" r="3"/><path d="M9 6h6M7.5 8.5 10 16M16.5 8.5 14 16"/>',
  key:        '<circle cx="8" cy="14" r="4"/><path d="m11.5 11 9-9M16 6l3 3"/>',
  doc:        '<path d="M5 3h10l5 5v13H5z"/><path d="M14 3v6h6"/>',
  layers:     '<path d="M12 2 2 8l10 6 10-6zM2 14l10 6 10-6M2 11l10 6 10-6"/>',
  diff:       '<path d="M9 4v12a3 3 0 0 0 3 3h3M15 20l3-3-3-3M15 4h3v3"/>',
  link:       '<path d="M10 14a4 4 0 0 1 0-6l3-3a4 4 0 0 1 6 6l-1.5 1.5M14 10a4 4 0 0 1 0 6l-3 3a4 4 0 0 1-6-6l1.5-1.5"/>',
  stream:     '<path d="M3 6c4 0 4 4 8 4s4-4 8-4M3 12c4 0 4 4 8 4s4-4 8-4M3 18c4 0 4 4 8 4s4-4 8-4"/>',
  list:       '<path d="M8 6h12M8 12h12M8 18h12M4 6h.5M4 12h.5M4 18h.5"/>',
  inbox:      '<path d="M3 13h4a3 3 0 0 0 3 3h4a3 3 0 0 0 3-3h4M5 13l3-9h8l3 9v6H5z"/>',
  zap:        '<path d="M13 2 4 14h7l-1 8 10-12h-7l1-8z"/>',
};

window.icon = function(name, size, opts) {
  opts = opts || {};
  const body = window.ICONS[name];
  if (!body) return '';
  const w = size || 16;
  const stroke = opts.stroke ?? 2;
  const cls = opts.cls ? ` class="${opts.cls}"` : '';
  return `<svg width="${w}" height="${w}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="${stroke}" stroke-linecap="round" stroke-linejoin="round"${cls}>${body}</svg>`;
};

// Convenient when you want to inline SVG into innerHTML strings.
window.iconStr = window.icon;
