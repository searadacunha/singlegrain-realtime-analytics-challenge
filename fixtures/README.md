# Fixture

`event_sample.jsonl` is the synthetic dataset of the Engineer 004 challenge, copied unchanged from
https://github.com/ericosiu/beat-claude/blob/4b719e4501c8ff98fd3e641c46d3bffe34b9aaa5/challenges/engineer-004/fixtures/event_sample.jsonl
so that the analysis runs offline.

SHA-256: `1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235`

`python3 analyze_fixture.py --expect-sha256 <hash>` refuses to pass silently on any other file.
