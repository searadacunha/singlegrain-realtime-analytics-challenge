# Fixture

The analysis runs on the synthetic dataset of the Engineer 004 challenge, `event_sample.jsonl`, from
https://github.com/ericosiu/beat-claude/blob/4b719e4501c8ff98fd3e641c46d3bffe34b9aaa5/challenges/engineer-004/fixtures/event_sample.jsonl

That repository has no license file, so this repository does not redistribute the file. Fetch it with

    python3 tools/fetch_fixture.py

which downloads it at the pinned commit and writes it here only if its SHA-256 is
`1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235`.
