# Negative fixtures — deliberately WRONG JSON. Do not "fix", do not regenerate.

These files are **not goldens.** A golden is what the TypeScript contract emits
(`scripts/generate-goldens.ts`) and lives in `../Goldens/`, where the root gate's
`check:goldens` stage regenerates and diffs it. Nothing in this directory is
generated, nothing here is diffed against the contract, and the generator must never
be pointed at it — that separation is why it is a sibling directory rather than a
prefixed file inside `Goldens/`.

Each file here is wrong in one **specific, intended** way, and exists so an automated
test can assert that a guard **rejects** it. A check that only ever passes is not a
check (the `SCHEMA_LEDGER` lesson from #85): without a fixture that must fail, a
weakened comparison goes green and no one finds out.

These files are hand-written, so — unlike the generated goldens, which `.oxfmtrc.json`
exempts — they are formatted by the repo's own formatter. Whitespace is not contract
here either: the harness compares **parsed** JSON.

## `agent-null-supersedes.json`

The content of `Goldens/agent-original.json` key for key, plus `"supersedes": null`.

`supersedes` is a `Schema.optionalKey` field, so the contract **OMITS** the key when
the value is absent — it never sends `null` (`docs/contract-mirror.md`). This file is
therefore a shape the daemon does not produce and would not accept, and it is
precisely the shape a Swift mirror would emit if `Optional` were encoded as an
explicit null rather than an omitted key.

`EncodeAgreementTests.rejectsNullWhereTheContractOmitsTheKey` asserts the
encode-agreement harness reports it as a divergence. If a future change normalised a
missing key to `null` — the one "simplification" that would make the whole harness
vacuous while every test still passed — that assertion is what fails.

**It must stay wrong.** If this file ever stops failing the harness, the bug is in the
harness, not in the file.
