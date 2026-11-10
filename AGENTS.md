# GUFI agent notes

## Build

- Use a private build directory inside or alongside the repository. Do not create, modify, or reuse a build directory belonging to another user or workflow.
- Use CMake only to configure an empty build directory or to change a CMake setting: `cmake <path-to-repository-root>`. Add `-DDEP_BUILD_THREADS=<integer>` to enable parallel dependency builds.
- When configuring on macOS, use Homebrew-installed CMake, compilers, toolchain utilities, and dependencies instead of system tools. Always add `-DCMAKE_OSX_SYSROOT=macosx`.
- When configuring on Cygwin, also add `-DDEP_USE_JEMALLOC=Off`.
- Build with `make -j <integer>` or specify a focused target. Do not use CMake to build.

## Documentation

- To build LaTeX documentation, ensure `pdflatex` and ImageMagick `convert` are installed. Add `-DLATEX_BUILD=ON` only when creating an empty build directory or changing a CMake setting, then run `make pdf`.

## Testing

- After changing a test source, template, or expected-output file, run `make` from the build directory before running the test. Do not rerun CMake solely to regenerate test files; do not use CMake to build them.
- Regression tests share a working directory and remove common paths. Never run CTest and a generated regression script concurrently.
- Run the full suite with `ctest --test-dir <build-directory> --output-on-failure`.
- Run a targeted unit test via its executable in the build directory, unless the test requires a different working directory.
- Run every individual regression test using its corresponding generated `.sh` script in `<build-directory>/test/regression`. Use CTest only when running the full test suite.
- If expected regression output changes, investigate all differences first. For an intentional change, verify the generated `<name>.out`, copy it to the version-controlled `test/regression/<name>.expected`, run `make` from the build directory, and rerun the test. Never edit generated output files by hand.
- In any regression script that captures output, perform every uncaptured command and check before the final two commands. The final two commands must be, in order, a comparison of `${OUTPUT}` with the expected file using the configured diff command and removal of `${OUTPUT}`. For example:
  ```sh
  @DIFF@ @CMAKE_CURRENT_BINARY_DIR@/<name>.expected "${OUTPUT}"
  rm "${OUTPUT}"
  ```

## Change Safety

- Inspect existing uncommitted changes before editing. Do not overwrite, discard, or mix unrelated changes with the requested work. Edit source files, `.in` templates, and version-controlled regression expected files; do not edit generated files under the build directory.
- Review patches and verify that changed code compiles and runs correctly.
- Keep patches minimal and limited to the requested scope.
- Preserve existing empty lines and comments; do not remove or reformat them unless necessary for the requested change.

## Licensing

- Add the complete project license text to every version-controlled source or script file, including source and script templates such as `*.c.in`, `*.cpp.in`, `*.py.in`, and `*.sh.in`.
- Leave exactly three empty lines after the license header before the file’s content begins. These lines must contain no comment markers or other characters. A required interpreter directive, such as a shebang, may appear before the header. For a comment-based header, count the empty lines after the comment’s closing line.
- Do not replace the license text with a reference to `LICENSE.txt`.
- The license-header requirement does not apply to configuration or metadata files, binary files, `*.expected` regression-test files, `*.md` files, `*.patch` files, or `LICENSE.txt`.

## Code Style

- Use four spaces for indentation. Do not use tabs for indentation. Preserve tabs only when they are required by existing non-indentation content or syntax.
- In an `if` statement with multiple conditions, wrap each comparison in parentheses, even when they are not required. Do not add parentheses around non-comparison conditions, including function calls. A single condition does not need an extra pair of parentheses. For example: `if ((value_a == value_b) && (count > 0) && is_valid())`.

### C/C++ Common Style

- Place opening braces for functions, control statements, and loops on the same line; place them on the following line for `struct`, `union`, and `enum` declarations. Align closing braces with the owning statement or declaration, keeping `else` and `else if` on the preceding closing-brace line.
- For new code or intentional code cleanups, organize include directives into four groups: standard-library headers, external dependency headers, common project headers, and program-specific headers. External dependency headers are non-project, non-standard headers, including headers provided by bundled dependencies. Sort each group alphabetically. Use `<>` for standard-library and external dependency headers, and `""` for project headers. First-level headers directly under `include/` are common project headers; project headers directly inside subdirectories of `include/` are program-specific headers.

#### C

- Use `/* comment */` for single-line C comments, keeping the opening and closing delimiters on the same line. Use the project’s multiline `/*`/`*/` style with ` *`-prefixed content for comments spanning multiple lines; preserve indentation.
- When a hardcoded string literal’s length is needed, write the known length explicitly instead of calling `strlen` on a duplicate literal. Use `size_t` for the length and cast it explicitly when needed.

#### C++

- Use `//` for all new C++ comments, including comments spanning multiple lines. Preserve existing block comments unless intentionally cleaning them up.
- When using `std::string` and obtaining its raw string, use `c_str()` rather than `data()`.
- When a hardcoded string literal’s length is needed, write the known length explicitly instead of calling `strlen` on a duplicate literal. Use `std::size_t` for the length and cast it explicitly when needed.

### Python

- Keep imports near the top, allowing required bootstrap code before them; separate standard-library imports from project or third-party imports.
- Use `snake_case` for functions, variables, and module names. Do not impose a class-naming convention; follow the surrounding file.
- Run `pylint` only on changed Python files, identified from the Git diff, when available; match the repository’s existing configuration rather than introducing broad formatting changes.

### Shell Scripts

- Use Bash for new repository shell scripts unless a different interpreter is required. New Bash scripts should start with `#!/usr/bin/env bash`; preserve an existing intentional shebang when modifying an established script. Use `set -e` while preserving existing behavior when different control flow is required.
- Quote variable expansions unless splitting or pathname expansion is intentional, and preserve the existing shellcheck-compatible style.
- When available, run `shellcheck` only on shell scripts changed according to the Git diff.
