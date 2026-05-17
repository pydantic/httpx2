# Fuzzing HTTPX2

Coverage-guided harnesses for the parts of HTTPX2 that consume untrusted
server input: URL parsing, header parsing, and content-encoding decoders
(gzip, deflate, brotli, zstd).

Each `fuzz_*.py` module is an [Atheris](https://github.com/google/atheris)
harness. The harnesses run continuously under
[OSS-Fuzz](https://github.com/google/oss-fuzz); local runs require an Atheris
install built against a libFuzzer-enabled Clang.

## Local runs

```shell
uv pip install atheris
uv run python fuzz/fuzz_urlparse.py -atheris_runs=100000
uv run python fuzz/fuzz_headers.py  -atheris_runs=100000
uv run python fuzz/fuzz_decoders.py -atheris_runs=100000
```

Pass a corpus directory as a positional argument to persist interesting
inputs across runs:

```shell
mkdir -p fuzz/corpus_urlparse
uv run python fuzz/fuzz_urlparse.py fuzz/corpus_urlparse
```
