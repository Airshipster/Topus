# Topus publication watchdog

This is a standby recovery runner for the existing Topus publisher. It polls
the public GitHub Actions workflow status and runs the same `src/main.py` only
when no successful or active GitHub run exists within the configured window.

The normal GitHub publisher and the fallback both use the Topus Master lock and
the `(video_id, project)` publication rows. The fallback therefore does not
create a second independent publication ledger. Its emergency RSS and publish
window is 72 hours, so a temporary outage does not turn a delayed video into a
permanent omission.

The status endpoint is internal-only on port 8090. Events are retained in the
local SQLite database for seven days. No service-account value is copied into
the image or repository.
