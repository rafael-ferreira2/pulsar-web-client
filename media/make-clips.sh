#!/usr/bin/env bash
#
# Slice the demo screen recording into compressed MP4s + animated GIFs,
# one per feature highlight. The README references these by exact filename,
# so once this script has run successfully the README's <video> / <img>
# tags will start rendering instead of showing broken-link icons.
#
# Prerequisites:
#   brew install ffmpeg            # macOS
#   (or apt/yum install ffmpeg, etc.)
#
# Usage:
#   ./make-clips.sh                # everything: full hero MP4 + poster + 7 feature clips (MP4 + GIF)
#   ./make-clips.sh --probe        # just print the source duration & exit
#   ./make-clips.sh --full-only    # only the hero MP4 + poster (00-full-demo.*)
#   ./make-clips.sh --clips-only   # skip the hero, only build the per-feature clips
#   ./make-clips.sh --mp4-only     # skip the (heavier) GIF stage
#   ./make-clips.sh --gif-only     # skip MP4 transcode, only build GIFs
#
# To tune the per-scene timestamps, edit the CLIPS array near the bottom.
# Each row is:  start_seconds  duration_seconds  label
#
# Outputs land in ./clips/ (created on first run):
#   clips/00-full-demo.mp4         ← compressed full walkthrough (the README hero)
#   clips/00-full-demo-poster.jpg  ← still frame used as the <video poster> / fallback img
#   clips/<label>.mp4              ← H.264, ~720p — good for <video> embeds
#   clips/<label>.gif              ← palette-optimised, ~960px wide, fits ![]() embeds
#
# The committed MP4 is dramatically smaller than the source .mov (typically
# ~5-15× compression for screen-recording content) while staying crisp at
# the README's viewport size, so the whole demo can ship inside the repo
# without Git LFS. The README references everything in ./clips/ — never the
# raw .mov — so once this script has run, every video/GIF tag in the main
# README renders inline on github.com/<repo>.

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SOURCE_FILE="${SCRIPT_DIR}/ScreenRecording.mov"
readonly OUT_DIR="${SCRIPT_DIR}/clips"

# ── Colours (skip if not a TTY) ─────────────────────────────────────────────
if [[ -t 1 ]]; then
  c_reset=$'\033[0m'; c_dim=$'\033[2m'; c_bold=$'\033[1m'
  c_red=$'\033[31m'; c_grn=$'\033[32m'; c_ylw=$'\033[33m'; c_blu=$'\033[34m'
else
  c_reset=''; c_dim=''; c_bold=''; c_red=''; c_grn=''; c_ylw=''; c_blu=''
fi

log_ok()   { printf '%s✓%s %s\n' "${c_grn}" "${c_reset}" "$*"; }
log_info() { printf '%s•%s %s\n' "${c_blu}" "${c_reset}" "$*"; }
log_warn() { printf '%s!%s %s\n' "${c_ylw}" "${c_reset}" "$*"; }
log_err()  { printf '%s✗%s %s\n' "${c_red}" "${c_reset}" "$*" >&2; }

# ── Pre-flight ──────────────────────────────────────────────────────────────
require_ffmpeg() {
  if ! command -v ffmpeg >/dev/null 2>&1; then
    log_err "ffmpeg not found on PATH."
    cat <<EOF
${c_dim}Install it first:
  macOS    →  brew install ffmpeg
  Ubuntu   →  sudo apt install ffmpeg
  Fedora   →  sudo dnf install ffmpeg

Then re-run:  ./make-clips.sh${c_reset}
EOF
    exit 2
  fi
}

require_source() {
  if [[ ! -r "${SOURCE_FILE}" ]]; then
    log_err "Source file not found: ${SOURCE_FILE}"
    cat <<EOF
${c_dim}Drop your screen recording at:
  media/ScreenRecording.mov

Then re-run this script.${c_reset}
EOF
    exit 2
  fi
}

probe_duration() {
  ffprobe -v error -show_entries format=duration \
          -of default=noprint_wrappers=1:nokey=1 \
          "${SOURCE_FILE}"
}

# ── Encoders ────────────────────────────────────────────────────────────────
# H.264 MP4 — good balance of size vs quality for screen-recording content.
# `-pix_fmt yuv420p` keeps the output playable in *every* browser <video> tag.
# `-movflags +faststart` puts the moov atom up front so progressive download
# starts playing immediately on GitHub/Bitbucket without buffering the tail.
make_mp4() {
  local start="$1" dur="$2" label="$3" out="${OUT_DIR}/${3}.mp4"
  log_info "MP4  $(printf '%-14s' "${label}") ← ${start}s + ${dur}s → ${out#${SCRIPT_DIR}/}"
  ffmpeg -y -hide_banner -loglevel error \
    -ss "${start}" -i "${SOURCE_FILE}" -t "${dur}" \
    -vf "scale=1280:-2:flags=lanczos,fps=30" \
    -c:v libx264 -preset slow -crf 24 -pix_fmt yuv420p \
    -movflags +faststart \
    -an "${out}"
  log_ok   "MP4  $(printf '%-14s' "${label}") $(du -h "${out}" | awk '{print $1}')"
}

# Full-walkthrough hero MP4 — bigger than the per-feature clips but still
# tightly compressed so the whole README hero loads in seconds. 720p, 30 fps,
# CRF 26 is a sweet spot for screen-recording content. Aimed at < 100 MB to
# fit GitHub's per-file ceiling without LFS.
make_hero_mp4() {
  local out="${OUT_DIR}/00-full-demo.mp4"
  log_info "MP4  $(printf '%-14s' 'full demo')   ← full source → ${out#${SCRIPT_DIR}/}"
  ffmpeg -y -hide_banner -loglevel error \
    -i "${SOURCE_FILE}" \
    -vf "scale='min(1280,iw)':-2:flags=lanczos,fps=30" \
    -c:v libx264 -preset medium -crf 26 -pix_fmt yuv420p \
    -movflags +faststart \
    -an "${out}"
  log_ok   "MP4  $(printf '%-14s' 'full demo')   $(du -h "${out}" | awk '{print $1}')"
}

# Poster frame for the hero <video>. We grab a frame 6 seconds in — past
# the opening blank/splash but before any micro-flicker — and emit a JPEG
# scaled to the same width as the MP4 so the README looks consistent
# whether the player loads or only the fallback <img> renders.
make_hero_poster() {
  local out="${OUT_DIR}/00-full-demo-poster.jpg"
  log_info "POST $(printf '%-14s' 'poster')      ← @6s → ${out#${SCRIPT_DIR}/}"
  ffmpeg -y -hide_banner -loglevel error \
    -ss 6 -i "${SOURCE_FILE}" -frames:v 1 \
    -vf "scale='min(1280,iw)':-2:flags=lanczos" \
    -q:v 4 \
    "${out}"
  log_ok   "POST $(printf '%-14s' 'poster')      $(du -h "${out}" | awk '{print $1}')"
}

# Two-pass GIF via palettegen → paletteuse. Massively smaller and crisper
# than ffmpeg's default GIF encoder. The numbers below are tuned for a
# README inline-load budget (target ≤ 10 MB per GIF):
#   • 720px wide  → still readable but ~25% smaller than 960px
#   • 14 fps      → fluid enough for UI demos without the 18-fps file weight
#   • 160 colors  → screens are mostly UI chrome + monospace text; the diff
#                   table loses nothing visible vs the full 256-colour palette
#   • bayer_scale=5 → tighter dither pattern, ~10-15% smaller than scale=4
make_gif() {
  local start="$1" dur="$2" label="$3"
  local pal="${OUT_DIR}/.${3}.palette.png"
  local out="${OUT_DIR}/${3}.gif"
  log_info "GIF  $(printf '%-14s' "${label}") ← ${start}s + ${dur}s → ${out#${SCRIPT_DIR}/}"
  ffmpeg -y -hide_banner -loglevel error \
    -ss "${start}" -i "${SOURCE_FILE}" -t "${dur}" \
    -vf "fps=14,scale=720:-1:flags=lanczos,palettegen=max_colors=160:stats_mode=diff" \
    "${pal}"
  ffmpeg -y -hide_banner -loglevel error \
    -ss "${start}" -i "${SOURCE_FILE}" -t "${dur}" -i "${pal}" \
    -filter_complex "fps=14,scale=720:-1:flags=lanczos[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=5" \
    "${out}"
  rm -f "${pal}"
  log_ok   "GIF  $(printf '%-14s' "${label}") $(du -h "${out}" | awk '{print $1}')"
}

# ── The clips themselves ─────────────────────────────────────────────────────
# Tune `start` / `duration` once you've eyeballed the recording. Labels MUST
# match the README's <img>/<video> references — see media/README.md.
#
# Defaults assume a ~146-second recording with roughly:
#   0:05 → 0:15  picking a Pulsar topic and pressing ▶ Listen
#   0:25 → 0:35  details panel: connection info, decoder, response shape
#   0:50 → 1:00  sidebar STG/PRD filters, topic sort, env collapse
#   1:10 → 1:25  compare page: picking Source A + Source B, correlation key
#   1:35 → 1:55  schema match score, Structure/Values/Both tabs
#   1:55 → 2:10  side-by-side VSCode-style line diff
#   2:15 → 2:25  ⬇ Export ▾ menu: JSON / CSV / Markdown
#
#   start  dur  label
CLIPS=(
  "5     10   01-pick-topic-and-listen"
  "25    12   02-message-structure-and-details"
  "50    12   03-sidebar-filter-and-sort"
  "70    15   04-compare-pick-sources"
  "95    18   05-compare-structure-and-values-diff"
  "115   15   06-side-by-side-line-diff"
  "130   12   07-export-menu"
)

# ── Main ────────────────────────────────────────────────────────────────────
# `mode` controls which families run:
#   all        → hero + feature clips, both MP4 and GIF
#   probe      → just print source duration
#   full       → hero only (00-full-demo.* + poster), both encodes
#   clips      → feature clips only, both encodes
#   mp4 / gif  → encoder filter (orthogonal: still respects full/clips selection)
mode='all'
encoder='both'   # 'both' | 'mp4' | 'gif'
target='both'    # 'both' | 'full' | 'clips'
case "${1:-}" in
  --probe)      mode='probe' ;;
  --full-only)  target='full'  ;;
  --clips-only) target='clips' ;;
  --mp4-only)   encoder='mp4'  ;;
  --gif-only)   encoder='gif'  ;;
  --help|-h)
    sed -n '3,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# //;s/^#//'
    exit 0 ;;
  '') ;;
  *)
    log_err "Unknown flag: ${1}"
    exit 2 ;;
esac

require_ffmpeg
require_source

duration=$(probe_duration)
log_info "Source: ${SOURCE_FILE#${SCRIPT_DIR}/}  (${c_bold}${duration%.*}s${c_reset})"

if [[ "${mode}" == 'probe' ]]; then
  exit 0
fi

mkdir -p "${OUT_DIR}"

# Hero: compressed full demo + poster frame. Poster is JPEG so it always
# renders (every GitHub README image MIME is supported) — used both as the
# <video poster> and as the <img> fallback for renderers that strip <video>.
if [[ "${target}" == 'both' || "${target}" == 'full' ]]; then
  if [[ "${encoder}" == 'both' || "${encoder}" == 'mp4' ]]; then
    make_hero_mp4
  fi
  make_hero_poster   # always make the poster — it's tiny and the README needs it
fi

# Per-feature clips. We use `read` rather than parsing the array element so a
# clip with three space-separated fields stays readable in the array def
# above (no awkward IFS gymnastics).
if [[ "${target}" == 'both' || "${target}" == 'clips' ]]; then
  for row in "${CLIPS[@]}"; do
    read -r start dur label <<<"${row}"
    if [[ "${encoder}" == 'both' || "${encoder}" == 'mp4' ]]; then
      make_mp4 "${start}" "${dur}" "${label}"
    fi
    if [[ "${encoder}" == 'both' || "${encoder}" == 'gif' ]]; then
      make_gif "${start}" "${dur}" "${label}"
    fi
  done
fi

echo
log_ok "Outputs in ${OUT_DIR#${SCRIPT_DIR}/}/"
ls -lh "${OUT_DIR}" | awk 'NR>1 {printf "    %-46s %s\n", $NF, $5}'
