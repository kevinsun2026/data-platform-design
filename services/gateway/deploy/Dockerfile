# syntax=docker/dockerfile:1.6
# ----------------------------------------------------------------------------
# AIDP gateway — multi-stage build.
#
# Stage 1: build a static Go binary on a pinned base image.
# Stage 2: copy the binary into a distroless image for runtime.
#
# The distroless base keeps the image small and removes a shell,
# package manager, and setuid binaries, so the attack surface is
# limited to the gateway binary and its transitively linked
# glibc. The binary is fully static (-extldflags=-static) so the
# scratch-style base image can load it without a libc to resolve.
# ----------------------------------------------------------------------------

FROM golang:1.27-bookworm AS build
WORKDIR /src

# Pre-copy go.mod/go.sum so dependency downloads are cached
# across builds that only change source files.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy the rest of the source and build a static binary. The
# `-trimpath` flag strips local filesystem paths from the
# binary so the same source produces the same bytes regardless
# of where the build ran; `-ldflags` strips DWARF so the image
# is small.
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 \
    go build -trimpath -ldflags="-s -w -extldflags=-static" -o /out/gateway .

# ----------------------------------------------------------------------------
# Runtime: distroless static.
# ----------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

# Copy the gateway binary and nothing else.
COPY --from=build /out/gateway /usr/local/bin/gateway

# Distroless already runs as uid 65532; we mirror that here for
# documentation purposes (k8s pod spec should also set
# runAsNonRoot: true and runAsUser: 65532).
USER nonroot:nonroot

EXPOSE 8000

# Distroless' static-debian image has no shell, so ENTRYPOINT
# must be the JSON form referencing the binary directly.
ENTRYPOINT ["/usr/local/bin/gateway"]
