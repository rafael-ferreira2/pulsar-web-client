# `media/` — demo recording & feature clips

This folder holds the visual material the main `README.md` embeds.

```
media/
├── ScreenRecording.mov         # raw source (~188 MB, .gitignored — never pushed)
├── make-clips.sh               # ffmpeg helper that produces everything below
└── clips/                      # committed to the repo (≈ 50 MB total)
    ├── 00-full-demo.mp4        # hero walkthrough — what the README <video> plays
    ├── 00-full-demo-poster.jpg # poster frame for the hero player
    ├── 01-pick-topic-and-listen.{mp4,gif}
    ├── 02-message-structure-and-details.{mp4,gif}
    ├── 03-sidebar-filter-and-sort.{mp4,gif}
    ├── 04-compare-pick-sources.{mp4,gif}
    ├── 05-compare-structure-and-values-diff.{mp4,gif}
    ├── 06-side-by-side-line-diff.{mp4,gif}
    └── 07-export-menu.{mp4,gif}
```

## Quick start

```sh
brew install ffmpeg            # one-time (or apt/yum install ffmpeg on Linux)
./make-clips.sh                # produces everything in ./clips/
```

That single command yields:

| Output | Used for |
|---|---|
| `clips/00-full-demo.mp4` (~5 MB) | The hero `<video>` at the top of the main README. |
| `clips/00-full-demo-poster.jpg` (~75 KB) | The poster shown before the hero plays, and the click-to-play fallback for renderers that strip `<video>`. |
| `clips/0N-*.mp4` (~200–700 KB each) | Higher-quality version of each feature clip — opened when a viewer clicks any of the GIFs in the README grid. |
| `clips/0N-*.gif` (~2–11 MB each) | Inline auto-playing previews in the README's feature highlight table — these are what most viewers actually watch. |

## Script flags

```sh
./make-clips.sh                 # everything: hero + 7 feature clips, MP4 + GIF
./make-clips.sh --probe         # just print the source duration & exit
./make-clips.sh --full-only     # only rebuild the hero (00-full-demo.* + poster)
./make-clips.sh --clips-only    # skip the hero, only build the per-feature clips
./make-clips.sh --mp4-only      # skip the (heavier) GIF stage (fast iteration)
./make-clips.sh --gif-only      # skip MP4 transcode, only build GIFs
./make-clips.sh --help          # show inline docs
```

The script does graceful pre-flight: missing `ffmpeg` or missing source file → friendly error with install hints, no crash.

## Tuning the clip ranges

If your recording is shorter, longer, or shows the steps in a different order, edit the `CLIPS=( … )` array near the bottom of `make-clips.sh`. Each row is:

```
start_seconds  duration_seconds  label
```

The `label` must match what the main `README.md` references (e.g. `01-pick-topic-and-listen`), otherwise the clip won't appear in the rendered doc. Tip: run `./make-clips.sh --probe` first to confirm your source's total duration, then iterate with `--mp4-only` while you dial in the timestamps (MP4 is ~10× faster to produce than GIF).

## Encoder details

- **Hero MP4** — H.264 / yuv420p, `-preset medium -crf 26`, scaled to ≤ 1280 px wide, 30 fps, `+faststart` so GitHub's player streams it progressively. ~5 MB for a 2:26 screen recording. Audio stripped.
- **Clip MP4** — same encoder, `-preset slow` (worth it for ~15 s clips), Lanczos resample.
- **Clip GIF** — two-pass `palettegen → paletteuse` (massively crisper than ffmpeg's default GIF encoder). 720 px wide, 14 fps, 160-colour palette, Bayer dither (`bayer_scale=5`). Each GIF lands in the 2–11 MB range, balancing quality against README load time.

## Why no `ScreenRecording.mov` in git

The raw QuickTime source is ~188 MB — above GitHub's 100 MB hard ceiling for normal git objects. Three ways to handle that, in increasing order of repo intrusion:

1. **Keep it out of git** *(current setup — `.gitignored`)*. Treat it as the source-of-truth on the recording author's disk; commit only the smaller compressed outputs. If someone else needs to regenerate, they re-record their own walkthrough.
2. **Host externally** (Drive, Confluence, S3) and link from `README.md`. Useful if you want a shareable "raw" copy without bloating the repo.
3. **Git LFS** — supported on GitHub and Bitbucket Cloud, counts against your LFS storage quota:

   ```sh
   git lfs install
   git lfs track "media/ScreenRecording.mov"
   git add .gitattributes media/ScreenRecording.mov
   git commit -m "Track demo recording via LFS"
   ```

   Then **remove** `media/ScreenRecording.mov` from `.gitignore`.

## Optional: 100% inline autoplay via GitHub `user-attachments`

The current setup gives you a `<video>` player on GitHub (it works for repo-path MP4s in most cases). If you want *guaranteed* inline-everywhere autoplay (Edge, mobile Safari, embeds in PR comments), use the **`user-attachments` trick**:

1. Open a new issue (or any PR comment) in your repo.
2. **Drag & drop** `clips/00-full-demo.mp4` into the comment box. GitHub uploads it and replaces it with a URL like `https://github.com/user-attachments/assets/<uuid>`.
3. Copy that URL.
4. In `README.md`, replace the `<video src="./media/clips/00-full-demo.mp4" ...>` line with `<video src="https://github.com/user-attachments/assets/<uuid>" ...>`.

Why bother: GitHub serves user-attachments through a CDN with broader codec support than `<video>` tags pointing at repo paths, so the player works in more browsers/renderers. The cost is one manual upload per refresh.

## License / privacy

The recording shows the FanDuel-branded local UI but no real broker secrets — `tokens.sh` is git-ignored and the demo runs against staging-shaped placeholder data. Still: confirm nothing in your recording reveals credentials before sharing the repo widely.
