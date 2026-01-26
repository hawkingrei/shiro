# Testing Database Engines via Query Plan Guidance (QPG)

## Background
Random SQL fuzzing tends to repeat queries with similar plan shapes, leading to stalled coverage. Even with many queries, it may miss important optimizer branches and edge paths.

## Core Idea
QPG uses plan shape as a coverage signal. It extracts plan features via EXPLAIN and dynamically adjusts query generation to prioritize under-covered plan shapes.

## Key Mechanics
1. Run EXPLAIN for generated queries and extract plan node sequences, operator types, and join orders.
2. Maintain a cache of seen plan shapes and their frequencies to detect coverage stall.
3. When coverage stalls, boost generation probabilities for specific structures (e.g., joins, aggregates, subqueries).

## Oracle Role
QPG is not a correctness oracle. It is a coverage driver that biases generation toward rare plan shapes so other oracles can find more bugs.

## Scope and Limitations
- Requires EXPLAIN output that is stable and parseable.
- Overly strong guidance can bias the space; balance with random exploration.
- Plan cache and parameterized queries require special handling to avoid misleading signals.

## Impact
QPG lifts fuzzing from "input diversity" to "plan diversity," improving optimizer branch coverage and overall bug yield.
