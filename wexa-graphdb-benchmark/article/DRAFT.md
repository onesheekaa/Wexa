# Article draft — outline only, write the real thing after you have real numbers

Worth taking seriously: "communication" is 20% of the grade, the same weight as
"completeness of metrics." A great harness with a dry README and no article underdelivers
on a quarter of the rubric. This file is a skeleton, not a placeholder to skip.

**Target length:** 1,000–1,500 words. Publish on dev.to, Hashnode, or a personal blog,
and link it from the README. The rubric explicitly rewards engagement (stars/reactions/
views), not just "it exists" — so share it somewhere people who care about graph DBs
will actually see it (relevant subreddits, HN "Show HN", relevant Discord/Slack
communities), not just post-and-forget.

---

## Suggested structure

**1. Hook (2-3 sentences)**
Not "I benchmarked 5 graph databases." Something with a concrete surprising number
pulled from your actual results — e.g. "CognoDB and Memgraph both run Cypher. On the
same laptop, same query, same free-tier RAM cap, one of them was Nx faster. Here's why
that gap exists — and where the RAM cap alone explains it."

**2. Why this comparison, and why fairness is the hard part**
Explain the free-tier-parity constraint in plain language before showing any numbers —
readers need to trust the methodology before they trust the chart. This is where the
self-hosted-vs-cloud reasoning from README section 1 gets a human explanation instead of
a bullet list.

**3. The five contenders, one paragraph each**
Not a spec dump — what's architecturally different about each one (in-memory vs.
disk-backed, single-model vs. multi-model, centralized vs. distributed) and why that
difference is a hypothesis worth testing, not just a fact to state.

**4. The workloads, briefly**
One paragraph. Traversals, lookups, aggregation, mixed read/write, ingest. Don't
re-explain the whole methodology — link to the README for the full matrix.

**5. The results — pick 2-3 charts, not all of them**
Whichever findings are most surprising or most clearly explained by architecture.
Every claim about "why" needs the same honesty standard as the README's caveats
section — if you're not sure why a number came out the way it did, say so instead of
inventing a story.

**6. What actually explains the differences**
This is the section that separates a good article from a great one. Connect specific
numbers back to specific architectural choices — e.g. "Memgraph's 3-hop p95 stayed flat
under the 256MB cap because the entire graph fits in memory at this dataset size; Neo4j's
climbed because page-cache eviction started competing with the heap." Root-cause
reasoning, not just "and here's another chart."

**7. Honest limitations**
Small dataset, one geographic region, free-tier throttling if you hit it, anything from
the README's caveats section that's worth a reader knowing before they draw broader
conclusions from a 256MB-RAM benchmark.

**8. Closing — point back to the repo**
Anyone should be able to clone it and reproduce every number in the post.

---

## Notes to self before publishing

- Every number in the article must trace back to a number in `RESULTS.md` — no
  rounding for drama, no cherry-picking the metric that makes the story cleanest without
  saying so.
- Read it out loud once. If a sentence only makes sense to someone who already knows
  what Bolt or AQL is, rewrite it.
