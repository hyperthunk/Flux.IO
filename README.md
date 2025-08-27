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

reportgenerator -reports:src/Flux.IO.Tests/TestResults/b5076336-48c2-4cc9-a236-b3e3c257f24a/coverage.cobertura.xml -targetdir:coveragereport -reporttypes:Html

