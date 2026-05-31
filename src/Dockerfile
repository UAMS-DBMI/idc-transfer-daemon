# syntax=docker/dockerfile:1.7

# ---- build stage ----------------------------------------------------------
# Pinned to the toolchain declared in go.mod. Bump together.
FROM golang:1.26.2-bookworm AS build

WORKDIR /src

# Cache module downloads in a separate layer.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# Fully static binary so it runs on distroless/static (no libc, no cgo).
# -trimpath strips host paths from the binary; -ldflags shrinks it a bit.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" \
        -o /out/idc-transfer-daemon .

# ---- runtime stage --------------------------------------------------------
# distroless/static ships ca-certificates and /etc/passwd with a nonroot user,
# which is everything a static Go HTTPS client needs.
FROM gcr.io/distroless/static-debian12:nonroot
# FROM gcr.io/distroless/static-debian12:debug

COPY --from=build /out/idc-transfer-daemon /usr/local/bin/idc-transfer-daemon

# USER nonroot:nonroot
USER 1000:2123

ENTRYPOINT ["/usr/local/bin/idc-transfer-daemon"]
