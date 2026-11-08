"""Bound catch-up work while leaving room for fresh Push events."""

EVENT_BATCH_LIMIT = 100


def prioritize_events(events, limit=EVENT_BATCH_LIMIT, science_channels=()):
    if limit <= 0:
        return []
    science = set(science_channels)
    ordered = sorted(enumerate(events), key=lambda pair: (
        0 if (pair[1].get('channel_id') in science or
              str(pair[1].get('retry_project') or '').casefold() == 'scitopus') else 1,
        pair[0],
    ))
    push, background = [], []
    for _index, event in ordered:
        source = str(event.get('source') or '').strip().casefold()
        (push if source.startswith('push') else background).append(event)
    reserve = min(len(background), max(1, limit // 4)) if limit > 1 else 0
    first = min(len(push), limit - reserve)
    selected = push[:first] + background[:limit - first]
    if len(selected) < limit:
        selected.extend(push[first:first + limit - len(selected)])
    return selected
