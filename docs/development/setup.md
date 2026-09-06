# Setup

Tooling requirements for ServerFlow development.

## Required

| Tool | Version | Purpose |
| --- | --- | --- |
| Go | 1.24+ | primary language |
| Git | any | version control |

## Optional / Later Phases

| Tool | Phase | Purpose |
| --- | --- | --- |
| Docker + Compose | 16 | multi-service local deployment |
| Python 3.10+ | 1 | vLLM runtime tooling |
| vLLM | 1 | inference runtime (GPU required) |
| Kubernetes |  ​17 | cluster deployment |
| golangci-lint | any | stricter linting |

## Windows Notes

- `make` is not installed by default. Use the PowerShell quality gate
  instead, or run the Go commands directly:

```powershell
go build ./...
go vet ./...
go test ./...
```

- `go test -race` requires cgo, which needs a C compiler (e.g. MinGW
  gcc). Without one, run `go test ./...` only. CI runs the race
  detector on Linux.

## Module Path

The Go module path is currently `serverflow` because no git remote exists yet.

If a remote is added later, rename it in one step:

```bash
go mod edit -module github.com/<org>/serverflow
```

and update imports accordingly.

## First-Time Setup

```bash
go mod download
go build ./...
go vet ./...
go test ./...
```

On Windows, Go may be installed via winget:

```powershell
winget install --id GoLang.Go
```

A newly installed Go is not on `PATH` in already-open shells; open a
new terminal or use the full path `C:\Program Files\Go\bin\go.exe`.