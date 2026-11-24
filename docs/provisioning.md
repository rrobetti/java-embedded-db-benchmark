# Provisioning

## Reproducible normal path

Install JDK 21 or later and set `JAVA_HOME`; scripts verify the **major version**,
not merely the presence of `java`. The Maven wrapper supplies Maven.

```bash
bash scripts/setup.sh
java -Xmx1024m -cp target/benchmark.jar io.github.embeddedbench.db.DatabaseSmoke \
  --databases h2,hsqldb,sqlite,pglite
```

PowerShell 7: `pwsh -File scripts/setup.ps1`. Scripts print OS, CPU architecture,
logical CPUs and physical RAM, resolve with `dependency:go-offline`, package
and execute real connectivity. They do not install packages or use elevation.
Physical RAM is not necessarily a container's usable memory allowance.
The standalone connectivity JVM is capped at 1024m Java heap; package tests
use the limits configured by Maven/Surefire.

The pinned dependencies are in `pom.xml`: H2 2.5.252, HSQLDB 2.7.4,
Xerial sqlite-jdbc 3.54.1.0 and PGlite4j JDBC 0.0.3. PGlite4j is the published
Java/WASM artifact, **not** a Node.js PGlite package and **not** a local
PostgreSQL executable. Its runtime comes through Maven dependencies.
H2/HSQLDB are Java; SQLite requires extraction/loading of the native library
bundled in its JDBC jar.
PGlite4j uses Chicory, a pure-Java WASM runtime; it does not require a native
PGlite JNI library. The working benchmark adapter uses a local framed
transport repair over the real core/WASM and pgjdbc, not the unmodified
upstream JDBC driver/named URL. Its actual URL uses an internal
`127.0.0.1` pgjdbc socket; see [configuration](database-configuration.md).
The project overrides PGlite4j's transitive pgjdbc 42.7.5 with **42.7.15**
in dependency management: the older version has SCRAM CPU-exhaustion and
channel-binding-downgrade advisories. This override must pass actual
PGlite integration tests, not merely resolve in Maven.

## Offline and native diagnostics

After one successful online setup:

```bash
bash scripts/setup.sh --offline
bash scripts/benchmark.sh --offline --profile smoke
```

The wrapper distribution and plugin/dependency cache must exist. An offline
failure is not permission to silently download or substitute a database.
PowerShell setup takes `-Offline`; benchmark takes `--offline`.

Set `BENCHMARK_NATIVE_TMP` to a user-owned path when default extraction fails.
The launchers pass it as `java.io.tmpdir` and `org.sqlite.tmpdir`.
On Unix both directory-search and
write permissions are needed, and a `noexec` mount may block native loading.
On Windows inspect DLL architecture, antivirus/application-control logs and
access permissions. Do not disable security policy globally, `chmod 777`, or
install system packages with elevation from these scripts.

## Optional source rebuilds

Native source builds are **not required** for normal supported platforms.
Xerial's pinned release bundles Windows x86/x86_64/armv7/arm64, macOS
x86_64/arm64 and several Linux libc/musl architectures; use the
[release's platform matrix](https://github.com/xerial/sqlite-jdbc/blob/2b39e1f8b315fbe11cdd7271ee929ae8f9638e07/README.adoc)
instead of assuming every host is supported.

If a bundled SQLite binary cannot run on your platform, its JNI bridge and
SQLite **3.54.1** amalgamation must match the **3.54.1.0** driver. The
verified Xerial tag resolves to commit
`2b39e1f8b315fbe11cdd7271ee929ae8f9638e07`.
Optional upstream build:

```bash
git clone https://github.com/xerial/sqlite-jdbc.git sqlite-jdbc-source
git -C sqlite-jdbc-source checkout --detach 2b39e1f8b315fbe11cdd7271ee929ae8f9638e07
(cd sqlite-jdbc-source && make native)
```

[Upstream prerequisites](https://github.com/xerial/sqlite-jdbc/blob/2b39e1f8b315fbe11cdd7271ee929ae8f9638e07/CONTRIBUTING.md)
include JDK 11 for its build, Perl, Maven, make, gcc, curl and unzip.
Docker is only required for its cross-compilation target `make native-all`;
Windows source builds recommend WSL2. Provision tools manually. Do not
replace the bridge with an unrelated system SQLite library.

PGlite4j **0.0.3** is available from Maven; optional source revision:
`dd28e96c1b2bb0540b89a0125416d390f69aeb9b` (verified upstream `0.0.3` tag).
Its [pinned README](https://github.com/roastedroot/pglite4j/blob/dd28e96c1b2bb0540b89a0125416d390f69aeb9b/README.md)
documents a Dockerized WASM build:

```bash
git clone https://github.com/roastedroot/pglite4j.git pglite4j-source
git -C pglite4j-source checkout --detach dd28e96c1b2bb0540b89a0125416d390f69aeb9b
(cd pglite4j-source/wasm-build && make build && make unpack)
(cd pglite4j-source && mvn install)
```

This optional upstream build is not run by setup/CI and is not needed for
the published artifact. The pinned build script defaults to PostgreSQL 17.5
and branch `REL_17_5_WASM-pglite`; upstream tool/image/source fetches are
not all content-addressed. Pin their resolved commits/digests too before
claiming a byte-reproducible custom build.
If Docker/toolchains are unavailable, use the published artifact, not an
unpinned branch, Node server, external PostgreSQL or a mock.

Preserve source revisions, compiler flags, artifact digests and license
notices for custom builds. Repackage/rebuild the benchmark against the
replacement intentionally and repeat real connectivity, schema, transaction
and correctness tests. Changing only a source checkout does not replace the
dependency already inside `target/benchmark.jar`.

## Licenses

Repository code: Apache-2.0. H2: MPL-2.0 / EPL-1.0 dual license;
HSQLDB: its BSD-style license; SQLite engine: public domain;
Xerial's Java/JNI wrapper: Apache-2.0. PGlite4j's
[pinned LICENSE](https://github.com/roastedroot/pglite4j/blob/dd28e96c1b2bb0540b89a0125416d390f69aeb9b/LICENSE)
is Apache-2.0; Chicory is Apache-2.0, PostgreSQL engine code retains the
PostgreSQL license, and patched/bundled components retain their notices.
The shaded jar preserves dependency license/notice resources.
Review the actual redistributed artifacts and upstream notices, not just
this summary, before distribution.
