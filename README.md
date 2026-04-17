# abs-organizer-rs
Web based tool to sort abs files into a structure abs likes using symlinks.

# test files
This repo includes a simulated filesystem for testing the backend without real source files.

Usage:

- Start the server in testing mode (devcontainer sets `TESTING=1`):

```bash
cargo run
```

- The server will use `./simulated-abb` as `ABB_SRC` and `./simulated-abb-sorted` as `ABB_DST` by default when `TESTING=1`.

- You can edit or add files under `simulated-abb/` to create test bundles. Empty files are sufficient.

- Assignments in testing mode are recorded in `./.tmp/testing-state.json`.

Commands to create placeholders locally:

```bash
mkdir -p simulated-abb/"Ada Palmer - Terra Ignota (2016–2021)"
touch simulated-abb/"Ada Palmer - Terra Ignota (2016–2021)"/"Too Like the Lightning (2016) [Terra Ignota 1].m4b"
```