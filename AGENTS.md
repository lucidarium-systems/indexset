# Working agreement — indexset

The operating contract for **any** coding agent working in this repository. Codex, Cursor
and Gemini CLI read `AGENTS.md` natively; Claude Code loads it through the `@AGENTS.md`
import in [`CLAUDE.md`](CLAUDE.md). Never fork these rules into a per-vendor file.

**Rust** crate.

## Invariants (do not break these)

- **No Python.** Not a script, not `python3 -c`, not a heredoc. Reaching for it is the
  tell that a step is being solved by parsing when the tool that owns the answer could
  just be asked. Do not swap it for another parser either, and do not assume `jq` is
  present: it does not ship with macOS. A fixed-shape field is one `sed -nE` line;
  anything needing real parsing belongs in Rust, where it can be tested. If a task seems
  to need Python, the approach is wrong.
