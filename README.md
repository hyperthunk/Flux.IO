# Flux.IO
Async streaming &amp; parallel execution

### Test Suites

Running tests with coverage output can be done like so:

```bash
$ dotnet test src/Flux.IO.Tests --collect:"XPlat Code Coverage"
```

Take the cover guid for the run when producing test reports:

```bash
$ reportgenerator -reports:src/Flux.IO.Tests/TestResults/<test-run>/coverage.cobertura.xml -targetdir:coveragereport -reporttypes:Html
```

reportgenerator -reports:src/Flux.IO.Tests/TestResults/c02aeaf2-c71b-4da3-bf81-d2ff414ddda0/coverage.cobertura.xml -targetdir:coveragereport -reporttypes:Html