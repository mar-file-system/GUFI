# GUFI agent notes

## Build

- Use a private build directory inside or alongside the repository; do not create, modify, or reuse one belonging to another user or workflow.
- Use CMake only to configure an empty build directory or change a setting: `cmake <path-to-repository-root>`. Add `-DDEP_BUILD_THREADS=<integer>` for parallel dependency builds.
- When configuring on macOS, use Homebrew-installed CMake, compilers, toolchain utilities, and dependencies instead of system tools. Always add `-DCMAKE_OSX_SYSROOT=macosx`.
- When configuring on Cygwin, also add `-DDEP_USE_JEMALLOC=Off`.
- Build with `make -j <integer>` or a focused target, never with CMake.

## Documentation

- To build LaTeX documentation, ensure `pdflatex` and ImageMagick `convert` are installed; add `-DLATEX_BUILD=ON` only when creating an empty build directory or changing a CMake setting, then run `make pdf`.

## Testing

- After changing a test source, template, or expected-output file, run `make` from the build directory before running tests. Do not rerun CMake solely to regenerate test files, and do not use CMake to build them.
- Regression tests share a working directory and remove common paths. Never run CTest and a generated regression script concurrently.
- Run the full suite with `ctest --test-dir <build-directory> --output-on-failure --verbose` so failures include their complete output without requiring a rerun.
- Run targeted unit tests via their build-directory executables unless a different working directory is required.
- Run every individual regression test using its corresponding generated `.sh` script in `<build-directory>/test/regression`. Use CTest only when running the full test suite.
- If expected regression output changes, investigate all differences first. For an intentional change, verify the generated `<name>.out`, copy it to the version-controlled `test/regression/<name>.expected`, run `make` from the build directory, and rerun the test. Never edit generated output files by hand.
- In any regression test script that captures output in `${OUTPUT}`, perform every uncaptured command and check before the final two commands. Those commands must be, in order, a comparison of `${OUTPUT}` with the expected file using the configured diff command and removal of `${OUTPUT}`. Helper and setup scripts that do not capture `${OUTPUT}` are exempt. For example:
  ```sh
  @DIFF@ @CMAKE_CURRENT_BINARY_DIR@/<name>.expected "${OUTPUT}"
  rm "${OUTPUT}"
  ```

## Change Safety

- Inspect existing uncommitted changes before editing. Do not overwrite, discard, or mix unrelated changes with the requested work. Edit source files, `.in` templates, and version-controlled regression expected files; do not edit generated files under the build directory.
- When auditing changed files, use the current working tree as it exists, including both staged and unstaged tracked changes, and inspect every file in the requested scope from its complete current contents. Do not use another branch or an assumed branch baseline to define the scope.
- Review patches and verify that changed code compiles and runs correctly.
- Keep patches minimal and limited to the requested scope.
- Preserve existing empty lines and comments; remove or reformat them only when necessary for the requested change.

## Licensing

- Add the complete project license text to every version-controlled source or script file, including version-controlled generated source and script files and templates such as `*.c.in`, `*.cpp.in`, `*.py.in`, and `*.sh.in`.
- Leave exactly three empty lines after the license header before the file’s content begins. These lines must contain no comment markers or other characters. A required interpreter directive, such as a shebang, may appear before the header. For a comment-based header, count the empty lines after the comment’s closing line.
- Do not replace the license text with a reference to `LICENSE.txt`.
- The license-header requirement does not apply to configuration or metadata files, binary files, `*.expected` regression-test files, `*.md` files, `*.patch` files, or `LICENSE.txt`.

## Code Style

- Use four-space indentation; no indentation tabs or trailing whitespace. Preserve required tabs only in non-indentation content or syntax, never as trailing whitespace.

### C/C++ Common Style

- Use language-appropriate C and C++ declaration and definition syntax, same-line opening braces, and traditional Unix/POSIX conventions.
- Place opening braces for functions, control statements, loops, `struct`, `union`, and `enum` declarations on the same line. For constructs that use parentheses, put a space between the keyword and its opening parenthesis. Use curly braces for every control block, including single-line blocks, except that a `for` or `while` loop with an empty body may omit braces; when braces are omitted, place the empty-body semicolon on the same line as the loop. Align closing braces with the owning statement or declaration. Place `else` and `else if` on the line after the preceding closing brace.
- In an `if` statement with multiple conditions, wrap each comparison in parentheses, even when they are not required. Do not add parentheses around non-comparison conditions, including function calls. A single condition does not need an extra pair of parentheses. For example: `if ((value_a == value_b) && (count > 0) && is_valid())`.
- Separator-only comments, such as `/* --- */` or `// ---`, remain separate from adjacent descriptive comments.
- For new code or intentional code cleanups, organize include directives into four groups: standard-library headers, external dependency headers, common project headers, and program-specific headers. External dependency headers are non-project, non-standard headers, including headers provided by bundled dependencies. Sort each group alphabetically. Use `<>` for standard-library and external dependency headers, and `""` for project headers. First-level headers directly under `include/` are common project headers; project headers directly inside subdirectories of `include/` are program-specific headers.

#### C

- Use ANSI/ISO C prototype style.
- Use `/* ... */` for C comments and keep single-line comments on one line. A blank source line separates comment groups; convert each group to one `/* ... */` block, prefix every content line with ` *` and add a space after it for nonblank lines, put `*/` on its own line, do not split groups into single-line blocks, and preserve commented-out code and embedded comments exactly.
- When a hardcoded string literal’s length is needed, write the known length explicitly instead of calling `strlen` on a duplicate literal. Use `size_t` for the length and cast it explicitly when needed.

#### C++

- Use standard C++ function declaration and definition syntax.
- Use `//` for all new C++ comments, including multiline comments; start every comment line, including empty ones, with `//`. Preserve existing block comments unless intentionally cleaning them up.
- When obtaining a raw string from `std::string`, use `c_str()` rather than `data()`.
- When a hardcoded string literal’s length is needed, write the known length explicitly instead of calling `strlen` on a duplicate literal. Use `std::size_t` for the length and cast it explicitly when needed.

### Python

- Keep imports near the top, allowing required bootstrap code before them; separate standard-library imports from project or third-party imports.
- Use `snake_case` for functions, variables, and module names. Do not impose a class-naming convention; follow the surrounding file.
- Run `pylint` only on changed Python files, identified from the Git diff, when available; match the repository’s existing configuration rather than introducing broad formatting changes.

### Shell Scripts

- Use Bash for new repository shell scripts unless another interpreter is required; start new Bash scripts with `#!/usr/bin/env bash`, preserve any intentional existing shebang when modifying an established script, and use `set -e` unless existing control flow requires otherwise.
- Quote variable expansions unless splitting or pathname expansion is intentional, and preserve the existing shellcheck-compatible style.
- When available, run `shellcheck` only on shell scripts changed according to the Git diff.
