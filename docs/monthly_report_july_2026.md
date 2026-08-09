# Monthly Progress Report — July 2026

**Project:** GroundLoop FYP  
**Reporting period:** July 2026

## Overview

During July, I started and developed the main foundation of my Final Year
Project, GroundLoop. GroundLoop is a self-updating retrieval-augmented
generation system. Its goal is to keep the evidence behind an answer up to date
when documents are added, deleted, or replaced. Instead of rebuilding every
answer after every change, the system tries to find only the claims and answers
that may be affected and update those parts.

My main achievement this month was moving the project from an early idea to a
working and tested research prototype. I completed the first four major stages
of the project. These stages covered the project design, the basic data model,
incremental updates, the first real AI pipeline, and a dynamic update runtime.
I also created detailed documentation and tests so that every important result
can be checked again.

## Choosing and designing the project

At the start of the month, I compared several possible research directions. I
selected GroundLoop because it gives a good balance between databases,
incremental view maintenance, artificial intelligence, and a practical FYP
demo. I decided not to build a general agent-memory system because that topic
was too broad and difficult to define correctly. I also kept GroundLoop
separate from my earlier Secure CROWN work because the two projects have
different goals and code requirements.

I then completed a detailed design review. This review helped me reduce the
scope and make the research claims more realistic. The final design uses small,
bounded evidence groups instead of unrestricted reasoning graphs. It also
separates uncertain AI decisions from exact database maintenance. Model outputs
are stored as versioned observations. GroundLoop can then update the relational
state exactly relative to those stored observations, but it does not claim that
the model output is objective truth.

This design work gave me a clear technical plan and a list of decisions that
must not be changed without a new review. It also gave me a full-recomputation
reference path, which is important because every incremental result can be
compared with a simpler independent result.

## Building the core update system

After freezing the design, I built the first complete vertical slice. It
included one document, one answer, one claim, one evidence observation, one
update, and one answer-status change. I added immutable identifiers, versioned
records, claim and answer states, evidence currency, and exact replay checks.

I then completed a hardening pass. The main improvement was failure atomicity.
If an event is invalid or fails during processing, it now leaves the live state
unchanged. Failed events do not consume identifiers or advance the revision.
I also added checks for duplicate records, incorrect text hashes, invalid chunk
indexes, and conflicting replays. This made the basic system much safer and
more predictable.

The next step was the incremental maintenance engine. I implemented signed
deltas so the system can add and remove the effect of changed evidence without
recomputing every claim and answer. I kept an independent Python reference
implementation and added a separate SQL oracle in PostgreSQL. These three
paths—the reference result, the incremental result, and the SQL result—were
tested against each other.

One large randomized test processed 100,000 events and checked equality after
every event. I also tested policy changes, evidence replacement, withdrawals,
and exact replay. The live PostgreSQL tests used PostgreSQL 16.14 and pgvector
0.8.5. This stage showed that the structured maintenance logic was correct for
the tested cases. I described the policy-update method carefully as a known
mechanism adapted to GroundLoop, not as a new state-of-the-art algorithm.

## Adding the AI pipeline

I then built the first complete AI-based grounding pipeline. It loads local
documents, creates chunks, retrieves relevant evidence, generates a cited
answer, extracts claims, and verifies each claim against the retrieved text.
The pipeline records the model version, prompts, settings, inputs, calibration,
scores, timing, and reused artifacts. Publication to PostgreSQL is atomic, so a
failed run does not publish a partial answer.

For retrieval, I used BGE embeddings with pgvector. For generation and claim
extraction, I used a small Qwen model. For verification, I fine-tuned and
temperature-calibrated a MiniLM-based three-way classifier. I also added exact
replay before model loading. In one real replay, the system reused all 17
existing artifacts and made no new model artifacts.

The first integrated answer was not fully supported because the verifier score
was below the fixed threshold. I kept this result instead of hiding it. The
public verifier evaluation also had important limits, including weak REFUTE
coverage and input truncation. Therefore, this stage proved that the full
pipeline worked, but it did not prove that the model was highly accurate.

## Dynamic updates and evaluation

The final major part of July was M4, the dynamic update runtime. I added
versioned jobs, affected-claim discovery, provisional working state, sparse
publication, retry and replay handling, PostgreSQL persistence, and model
provenance. I tested document insertion, deletion, and replacement with a
pinned model. After every event, the maintained state agreed with the
independent Python and SQL structured results. Reconnecting and replaying the
same event used zero new discovery, embedding, or verifier calls.

I also corrected an early complexity claim. The first formula was too simple
because it ignored sorting, witness materialisation, bytes, database indexes,
I/O, logging, and lock costs. I replaced it with a more careful bounded claim
and clearly stated that the project does not prove a general sublinear update
time or superiority over other systems.

The evaluation produced mixed results. The runtime and database correctness
tests passed, but the frozen verifier was weak on small evidence revisions. A
later training experiment improved some revision results but failed one of the
pre-registered retention gates. I recorded the final result as **NO_GO** and
kept the original verifier as the default. This was still useful because it
showed where the current AI component fails and prevented me from making a
strong claim that the evidence did not support.

## What I learned and next steps

This month taught me that system correctness and AI quality must be measured
separately. The database can maintain an exact result relative to stored model
judgments even when those model judgments are imperfect. I also learned the
importance of independent reference implementations, failure-atomic
transactions, reproducible model provenance, fixed evaluation rules, and
honest negative results.

At the end of July, the core M1–M4 implementation and bounded evaluation were
complete. The next stage is M5, which adds bounded evidence groups and exact
covering matching for claims that need several distinct pieces of evidence. I
will also continue recovery and reconnect testing, durable work and timing
accounting, controlled evaluation, and full closure checks. A larger human-
adjudicated study remains future work before I can make a strong claim about
real-world usefulness.
