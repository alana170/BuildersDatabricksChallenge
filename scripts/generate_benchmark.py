#!/usr/bin/env python3
"""Generate a deterministic, valid Shopping Sessions benchmark dataset."""
import argparse
import csv
import json
import random
from bisect import bisect_right
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path

EVENT_HEADER = (
    "event_id",
    "user_id",
    "ts",
    "ingest_ts",
    "event_type",
    "product_id",
    "price_cents",
)
CLICK_HEADER = ("click_id", "user_id", "campaign_id", "ts")
LINK_HEADER = ("user_id_a", "user_id_b")
CATEGORIES = tuple(f"category_{i:02d}" for i in range(20))
CHANNELS = tuple(f"channel_{i}" for i in range(5))
WINDOW_START = int(datetime(2026, 10, 1, tzinfo=timezone.utc).timestamp())
WINDOW_SECONDS = 14 * 24 * 60 * 60
CLICK_WINDOW_SECONDS = 7 * 24 * 60 * 60
SESSION_DURATION_SECONDS = 20 * 60


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=1_000_000)
    parser.add_argument("--clicks", type=int, default=250_000)
    parser.add_argument("--users", type=int)
    parser.add_argument("--products", type=int, default=50_000)
    parser.add_argument("--campaigns", type=int, default=2_000)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--output", type=Path, default=Path("/tmp/dbc-benchmark-data"))
    args = parser.parse_args()
    if args.events < 0 or args.clicks < 0:
        parser.error("--events and --clicks must be non-negative")
    if args.users is not None and args.users < 1:
        parser.error("--users must be positive")
    if args.products < 1 or args.campaigns < 1:
        parser.error("--products and --campaigns must be positive")

    args.output = args.output.resolve()
    repo_root = Path(__file__).resolve().parents[1]
    if args.output == repo_root or repo_root in args.output.parents:
        parser.error("keep generated benchmark data outside the repository (for example, under /tmp)")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error(f"output directory is not empty: {args.output}")
    return args


def make_person_weights(person_count):
    whale_count = min(5, max(0, person_count // 1_000))
    regular_count = person_count - whale_count
    regular_weights = [max(1, 1_000_000 // rank) for rank in range(1, regular_count + 1)]
    regular_total = sum(regular_weights)
    whale_weight = max(1, regular_total * 15 // 925)
    weights = [whale_weight] * whale_count + regular_weights

    cumulative = []
    total = 0
    for weight in weights:
        total += weight
        cumulative.append(total)
    return cumulative, total


def choose_person(rng, cumulative_weights, total_weight):
    return bisect_right(cumulative_weights, rng.randrange(total_weight))


def timestamp_text(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def open_parts(stack, directory, count, header):
    writers = []
    for part in range(count):
        path = directory / f"part-{part:05d}.csv"
        stream = stack.enter_context(path.open("w", encoding="utf-8", newline="", buffering=1 << 20))
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(header)
        writers.append(writer)
    return writers


def generate(args):
    rng = random.Random(args.seed)
    person_count = args.users if args.users is not None else max(1, args.events // 30)
    cumulative_weights, total_weight = make_person_weights(person_count)
    event_counts = [0] * person_count
    for _ in range(args.events):
        event_counts[choose_person(rng, cumulative_weights, total_weight)] += 1

    linked_count = person_count // 10
    user_ids = [f"u{person:08d}" for person in range(person_count)]
    alternate_ids = [
        f"{user_id}_alt" if person < linked_count else None
        for person, user_id in enumerate(user_ids)
    ]

    session_starts = []
    for count in event_counts:
        session_count = min(240, max(1, count // 4))
        stride = WINDOW_SECONDS // session_count
        starts = []
        for session in range(session_count):
            jitter = rng.randrange(-10 * 60, 10 * 60 + 1)
            start = WINDOW_START + session * stride + jitter
            starts.append(min(max(start, WINDOW_START), WINDOW_START + WINDOW_SECONDS - SESSION_DURATION_SECONDS))
        session_starts.append(starts)

    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    event_dir = output / "events"
    click_dir = output / "clicks"
    link_dir = output / "links"
    event_dir.mkdir()
    click_dir.mkdir()
    link_dir.mkdir()

    with ExitStack() as stack:
        event_writers = open_parts(stack, event_dir, 24, EVENT_HEADER)
        click_writers = open_parts(stack, click_dir, 8, CLICK_HEADER)
        link_writers = open_parts(stack, link_dir, 4, LINK_HEADER)

        link_index = 0
        for person in range(linked_count):
            link_writers[link_index % len(link_writers)].writerow(
                (user_ids[person], alternate_ids[person])
            )
            link_index += 1

        event_index = 0
        for person, count in enumerate(event_counts):
            starts = session_starts[person]
            for _ in range(count):
                session_start = starts[rng.randrange(len(starts))]
                event_ts = session_start + rng.randrange(SESSION_DURATION_SECONDS)
                ingest_ts = event_ts + rng.randrange(10 * 60 + 1)
                event_type = rng.randrange(100)
                if event_type < 3:
                    event_type = "purchase"
                    price = rng.randint(100, 50_000)
                elif event_type < 10:
                    event_type = "cart"
                    price = ""
                else:
                    event_type = "view"
                    price = ""
                user_id = alternate_ids[person] if alternate_ids[person] and rng.randrange(2) else user_ids[person]
                product_id = f"p{rng.randrange(args.products):06d}"
                row = (
                    f"e{event_index:012d}",
                    user_id,
                    timestamp_text(event_ts),
                    timestamp_text(ingest_ts),
                    event_type,
                    product_id,
                    price,
                )
                event_writers[rng.randrange(len(event_writers))].writerow(row)
                event_index += 1

        click_window_start = WINDOW_START - CLICK_WINDOW_SECONDS
        for click_index in range(args.clicks):
            person = choose_person(rng, cumulative_weights, total_weight)
            user_id = alternate_ids[person] if alternate_ids[person] and rng.randrange(2) else user_ids[person]
            click_ts = click_window_start + rng.randrange(WINDOW_SECONDS + CLICK_WINDOW_SECONDS)
            campaign_id = f"campaign_{rng.randrange(args.campaigns):04d}"
            click_writers[rng.randrange(len(click_writers))].writerow(
                (f"c{click_index:012d}", user_id, campaign_id, timestamp_text(click_ts))
            )

    with (output / "products.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(("product_id", "category"))
        for product in range(args.products):
            writer.writerow((f"p{product:06d}", CATEGORIES[product % len(CATEGORIES)]))

    with (output / "campaigns.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(("campaign_id", "channel"))
        for campaign in range(args.campaigns):
            writer.writerow((f"campaign_{campaign:04d}", CHANNELS[campaign % len(CHANNELS)]))

    manifest = {
        "window_start": timestamp_text(WINDOW_START),
        "window_end": timestamp_text(WINDOW_START + WINDOW_SECONDS),
    }
    (output / "manifest.json").write_text(json.dumps(manifest, separators=(",", ":")) + "\n")
    print(
        f"generated seed={args.seed} events={event_index} clicks={args.clicks} "
        f"users={person_count} links={linked_count} output={output}"
    )


if __name__ == "__main__":
    generate(parse_args())
