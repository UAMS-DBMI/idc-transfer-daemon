# syntax=docker/dockerfile:1.7

# ---- build stage ----------------------------------------------------------
# Pinned to the toolchain declared in go.mod. Bump together.
# --platform=$BUILDPLATFORM runs the compiler natively on the build host and
# cross-compiles for the target (set via `docker buildx --platform`), rather
# than emulating the target CPU for the whole build.
FROM --platform=$BUILDPLATFORM golang:1.26.2-bookworm AS build
ARG TARGETOS TARGETARCH

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
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
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
