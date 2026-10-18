"""Project-scoped publication deadlines; no global recovery-age override."""
import config


def publication_age_limit(project=None):
    project = project or {}
    name = str(project.get('name') or '').strip().casefold()
    ceiling = 168 if name == 'scitopus' else config.MAX_PUBLISH_AGE_HOURS
    try:
        value = int(project.get('max_publish_age_hours') or ceiling)
    except (TypeError, ValueError):
        value = ceiling
    return max(1, min(value, ceiling))
