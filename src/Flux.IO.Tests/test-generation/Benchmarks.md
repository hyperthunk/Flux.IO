markdown name=Benchmarks/README.md
# Assembler Benchmarks

## Running

From the test project (or a dedicated benchmarks project if you split it):

```
dotnet run -c Release --project src/Flux.IO.Tests/Benchmarks/Flux.IO.Tests.Benchmarks.fsproj
```

## Scenarios

| Scenario | Approx Size | Notes |
|----------|-------------|------|
| SmallFlat | ~100 B | Shallow object |
| MediumNested | ~25 KB | Repeated nested blocks |
| Large | ~400 KB | Stress allocation & scanning |
| Pathological | ~80 KB | Many small string props |

## Chunk Patterns

| Pattern | Description |
|---------|-------------|
| Single | Entire document as one chunk |
| UniformSmall | 64B slices |
| Mixed | Random (8–256B) chunk sizes |
| Byte | Worst-case 1B per chunk |

## Interpreting Results

- LengthGate will appear fastest for Single/Uniform (no structural scan during feed); loses relative benefit as chunk count grows.
- Framing adds per-byte structural scan overhead but stays close for small docs.
- StreamingMaterialize adds token construction overhead; compare allocations to assess trade-offs before adopting external token streaming.


### 6.5 (Optional) Refactoring Plan Appendix Update

If you want to track progress:



---

## 7. How to Integrate

1. Add the new files.
2. Add BenchmarkDotNet package to the test project (or separate bench project).
3. Run existing test suite; ensure new properties pass.
4. (Optional) Run benchmarks to establish baseline performance numbers before further Tier C refactors.

---