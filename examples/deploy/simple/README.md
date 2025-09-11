# Simple GUFI Server/Client

GUFI can be deployed as a combination of (1) dedicated indexing server, and (2) one or more client machines.

At a high level, this might conceptually look something like:

<img src="deployment-simple.svg">

In this example,
 1. Filesystems to be indexed are mounted on the **GUFI Server**.
 2. Indexes are created from these filesystems using **Index Creation** programs (e.g., `gufi_dir2index`, `gufi_trace2index`) and are placed into a location backed by dedicated, high-performant **Index Storage (NVMe)**.
 3. In-progress GUFI indexes are symlinked to locations in `/search_in_progress`, and then, upon completion, are symlinked to a location in `/search`. This results in atomic index updates from the point of view of the user.
 4. A **Restricted Shell** (`rbash`) is configured on the GUFI Server so that users can only run GUFI Server Programs on the GUFI Server.
 5. GUFI programs that query indexes are placed into `/gufi_bin`, and are executed via the Restricted Shell by users.
 6. A custom home directory (`/gufi_home`) is configured to set up the read-only user environment; this involves configuring read-only dotfiles that set up user environment variables (e.g., `PATH=/gufi_home`).
 7. Users can log into a **GUFI Client** and run **GUFI Client Programs**, which themselves access the GUFI Server to run the **GUFI Server Programs** used for querying GUFI indexes.
