import multiprocessing
import multiprocessing.dummy
import multiprocessing.pool
import os
import posixpath
import threading
from typing import Dict, List, NoReturn, Optional, Set, Tuple, Union

import dropbox  # type: ignore
import requests  # type: ignore  # a dependency of dropbox

from git_remote_dropbox import git
from git_remote_dropbox.constants import (
    BULK_FETCH_THRESHOLD,
    CHUNK_SIZE,
    MAX_RETRIES,
    PROCESSES,
)
from git_remote_dropbox.util import (
    Binder,
    Level,
    Poison,
    Token,
    find_dropbox_roots,
    readline,
    stderr,
    stdout,
)

try:
    # Importing synchronize is to detect platforms where
    # multiprocessing does not work (python issue 3770)
    # and cause an ImportError. Otherwise it will happen
    # later when trying to use Queue().
    from multiprocessing import Queue
    from multiprocessing import synchronize as _  # noqa: F401
except ImportError:
    from queue import Queue  # type: ignore


class Helper:
    """
    A git remote helper to communicate with Dropbox.
    """

    def __init__(self, token: Token, path: str, processes: int = PROCESSES) -> None:
        self._token = token
        self._per_thread = threading.local()
        self._path = path
        self._processes = processes
        self._verbosity = Level.INFO  # default verbosity
        self._refs: Dict[str, Tuple[str, str]] = {}  # map from remote ref name => (rev number, sha)
        self._pushed: Dict[str, str] = {}  # map from remote ref name => sha
        self._first_push = False
        self._local_dir_known = False
        self._local_dir_cached: Optional[str] = None

    @property
    def verbosity(self) -> Level:
        return self._verbosity

    def _trace(self, message: str, level: Level = Level.DEBUG, *, exact: bool = False) -> None:
        """
        Log a message with a given severity level.
        """
        if level > self._verbosity:
            return
        if exact:
            if level == self._verbosity:
                stderr(message)
            return
        if level <= Level.ERROR:
            stderr(f"error: {message}\n")
        elif level == Level.INFO:
            stderr(f"info: {message}\n")
        elif level >= Level.DEBUG:
            stderr(f"debug: {message}\n")

    def _fatal(self, message: str) -> NoReturn:
        """
        Log a fatal error and exit.
        """
        self._trace(message, Level.ERROR)
        # exit without interpreter teardown: daemon worker threads may still
        # be writing to stderr, and normal teardown aborts (SIGABRT) if it
        # cannot acquire the stderr buffer lock
        os._exit(1)

    @property
    def _connection(self) -> dropbox.Dropbox:
        """
        Return a Dropbox connection object private to this thread.

        Lazily initialized per-thread.
        """
        if not hasattr(self._per_thread, "connection"):
            self._per_thread.connection = self._token.connect()
        return self._per_thread.connection

    def run(self) -> None:
        """
        Run the helper following the git remote helper communication protocol.
        """
        while True:
            line = readline()
            if line == "capabilities":
                _write("option")
                _write("push")
                _write("fetch")
                _write()
            elif line.startswith("option"):
                self._do_option(line)
            elif line.startswith("list"):
                self._do_list(line)
            elif line.startswith("push"):
                self._do_push(line)
            elif line.startswith("fetch"):
                self._do_fetch(line)
            elif line == "":
                break
            else:
                self._fatal(f"unsupported operation: {line}")

    def _do_option(self, line: str) -> None:
        """
        Handle the option command.
        """
        if line.startswith("option verbosity"):
            self._verbosity = Level(int(line[len("option verbosity ") :]))
            _write("ok")
        else:
            _write("unsupported")

    def _do_list(self, line: str) -> None:
        """
        Handle the list command.
        """
        for_push = "for-push" in line
        refs = self.get_refs(for_push=for_push)
        for sha, ref in refs:
            _write(f"{sha} {ref}")
        if not for_push:
            head = self.read_symbolic_ref("HEAD")
            if head:
                _write(f"@{head[1]} HEAD")
            else:
                self._trace("no default branch on remote", Level.INFO)
        _write()

    def _do_push(self, line: str) -> None:
        """
        Handle the push command.
        """
        remote_head = None
        while True:
            src, dst = line.split(" ")[1].split(":")
            if src == "":
                self._delete(dst)
            else:
                self._push(src, dst)
                if self._first_push and (not remote_head or src == git.symbolic_ref_value("HEAD")):
                    remote_head = dst
            line = readline()
            if line == "":
                if self._first_push:
                    self._first_push = False
                    if remote_head:
                        if not self.write_symbolic_ref("HEAD", remote_head):
                            self._trace("failed to set default branch on remote", Level.INFO)
                    else:
                        self._trace("first push but no branch to set remote HEAD")
                break
        _write()

    def _do_fetch(self, line: str) -> None:
        """
        Handle the fetch command.
        """
        while True:
            _, sha, _ = line.split(" ")
            self._fetch(sha)
            line = readline()
            if line == "":
                break
        _write()

    def _delete(self, ref: str) -> None:
        """
        Delete the ref from the remote.
        """
        self._trace(f"deleting ref {ref}")
        head = self.read_symbolic_ref("HEAD")
        if head and ref == head[1]:
            _write(f"error {ref} refusing to delete the current branch: {head[1]}")
            return
        try:
            self._connection.files_delete(self._ref_path(ref))
        except dropbox.exceptions.ApiError as e:
            if not isinstance(e.error, dropbox.files.DeleteError):
                raise
            # someone else might have deleted it first, that's fine
        self._refs.pop(ref, None)  # discard
        self._pushed.pop(ref, None)  # discard
        _write(f"ok {ref}")

    def _push(self, src: str, dst: str) -> None:
        """
        Push src to dst on the remote.
        """
        force = False
        if src.startswith("+"):
            src = src[1:]
            force = True
        present = [self._refs[name][1] for name in self._refs]
        present.extend(self._pushed.values())
        # before updating the ref, write all objects that are referenced
        objects = git.list_objects(src, present)
        try:
            # upload objects in parallel
            pool = multiprocessing.pool.ThreadPool(processes=self._processes)
            res = pool.imap_unordered(Binder(self, "_put_object"), objects)
            # show progress
            total = len(objects)
            self._trace("", level=Level.INFO, exact=True)
            for done, _ in enumerate(res, 1):
                pct = int(float(done) / total * 100)
                message = f"\rWriting objects: {pct:3.0f}% ({done}/{total})"
                if done == total:
                    message = f"{message}, done.\n"
                self._trace(message, level=Level.INFO, exact=True)
        except Exception:
            if self.verbosity >= Level.DEBUG:
                raise  # re-raise exception so it prints out a stack trace
            self._fatal("exception while writing objects (run with -v for details)\n")
        sha = git.ref_value(src)
        error = self._write_ref(sha, dst, force=force)
        if error is None:
            _write(f"ok {dst}")
            self._pushed[dst] = sha
        else:
            _write(f"error {dst} {error}")

    def _ref_path(self, name: str) -> str:
        """
        Return the path to the given ref on the remote.
        """
        if not name.startswith("refs/"):
            msg = f"invalid ref name: {name}"
            raise ValueError(msg)
        return posixpath.join(self._path, name)

    def _ref_name_from_path(self, path: str) -> str:
        """
        Return the ref name given the full path of the remote ref.
        """
        prefix = f"{self._path}/"
        if not path.startswith(prefix):
            msg = f"invalid ref path: {path}"
            raise ValueError(msg)
        return path[len(prefix) :]

    def _object_path(self, name: str) -> str:
        """
        Return the path to the given object on the remote.
        """
        prefix = name[:2]
        suffix = name[2:]
        return posixpath.join(self._path, "objects", prefix, suffix)

    @property
    def _local_dir(self) -> Optional[str]:
        """
        Return this repository's directory inside a local Dropbox sync
        folder, or None if there is no such directory on this machine.

        Objects — which are immutable and verified by hash — may be read from
        this directory instead of being downloaded via the API. Refs must
        always go through the API: the sync folder can lag behind the server,
        and ref updates rely on the API's atomic compare-and-swap. Because
        every local read is hash-verified (with an API fallback on mismatch),
        a stale or even entirely wrong local folder can never corrupt a
        fetch. Set GIT_REMOTE_DROPBOX_NO_LOCAL to disable local reads.
        """
        if not self._local_dir_known:
            self._local_dir_known = True
            if not os.environ.get("GIT_REMOTE_DROPBOX_NO_LOCAL"):
                relative = self._path.strip("/").split("/")
                for root in find_dropbox_roots():
                    candidate = os.path.join(root, *relative)
                    if os.path.isdir(os.path.join(candidate, "objects")):
                        self._trace(f"using local dropbox folder: {candidate}")
                        self._local_dir_cached = candidate
                        break
        return self._local_dir_cached

    def _read_local_object(self, sha: str) -> Optional[bytes]:
        """
        Read an object's encoded data from the local Dropbox sync folder.

        Return None if there is no local folder or the object is not in it.
        The Dropbox client materializes online-only placeholder files on
        read. Callers must verify the hash of the returned data.
        """
        local_dir = self._local_dir
        if local_dir is None:
            return None
        path = os.path.join(local_dir, "objects", sha[:2], sha[2:])
        try:
            with open(path, "rb") as f:
                return f.read()
        except OSError:
            return None

    def _get_object(self, sha: str) -> Tuple[bytes, str, bytes]:
        """
        Get an object's encoded data, preferring the local Dropbox folder
        over the API.

        Return a tuple (encoded data, kind, contents); raise if the object
        cannot be obtained or its hash does not match.
        """
        local = self._read_local_object(sha)
        if local is not None:
            try:
                computed_sha, kind, contents = git.decode_object(local)
                if computed_sha == sha:
                    return (local, kind, contents)
            except Exception:  # noqa: BLE001, S110
                pass
            self._trace(f"invalid local copy of {sha}, downloading")
        _, data = self._get_file(self._object_path(sha))
        computed_sha, kind, contents = git.decode_object(data)
        if computed_sha != sha:
            msg = f"hash mismatch {computed_sha} != {sha}"
            raise ValueError(msg)
        return (data, kind, contents)

    def _get_file(self, path: str) -> Tuple[str, bytes]:
        """
        Return the revision number and content of a given file on the remote.

        Return a tuple (revision, content).
        """
        self._trace(f"fetching: {path}")
        retries = 0
        while True:
            try:
                meta, resp = self._connection.files_download(path)
            except (dropbox.exceptions.InternalServerError, requests.exceptions.ConnectionError):
                if retries >= MAX_RETRIES:
                    raise
                retries += 1
                self._trace(f"transient error fetching {path}, retrying")
            else:
                return (meta.rev, resp.content)

    def _get_files(self, paths: List[str]) -> List[Tuple[str, bytes]]:
        """
        Return a list of (revision number, content) for a given list of files.
        """
        pool = multiprocessing.dummy.Pool(self._processes)
        return pool.map(self._get_file, paths)  # type: ignore

    def _put_object(self, sha: str) -> None:
        """
        Upload an object to the remote.
        """
        data = git.encode_object(sha)
        path = self._object_path(sha)
        self._trace(f"writing: {path}")
        retries = 0
        mode = dropbox.files.WriteMode.overwrite

        if len(data) <= CHUNK_SIZE:
            while True:
                try:
                    self._connection.files_upload(data, path, mode, strict_conflict=True, mute=True)
                except (dropbox.exceptions.InternalServerError, requests.exceptions.ConnectionError):
                    self._trace(f"transient error writing {sha}, retrying")
                    if retries < MAX_RETRIES:
                        retries += 1
                    else:
                        raise
                else:
                    break
        else:
            cursor = dropbox.files.UploadSessionCursor(offset=0)
            done_uploading = False

            while not done_uploading:
                try:
                    end = cursor.offset + CHUNK_SIZE
                    chunk = data[(cursor.offset) : end]

                    if cursor.offset == 0:
                        # upload first chunk
                        result = self._connection.files_upload_session_start(chunk)
                        cursor.session_id = result.session_id
                    elif end < len(data):
                        # upload intermediate chunks
                        self._connection.files_upload_session_append_v2(chunk, cursor)
                    else:
                        # upload the last chunk
                        commit_info = dropbox.files.CommitInfo(path, mode, strict_conflict=True, mute=True)
                        self._connection.files_upload_session_finish(chunk, cursor, commit_info)
                        done_uploading = True

                    # advance cursor to next chunk
                    cursor.offset = end

                except dropbox.files.UploadSessionOffsetError as offset_error:
                    self._trace(f"offset error writing {sha}, retrying")
                    cursor.offset = offset_error.correct_offset
                    if retries < MAX_RETRIES:
                        retries += 1
                    else:
                        raise
                except (dropbox.exceptions.InternalServerError, requests.exceptions.ConnectionError):
                    self._trace(f"transient error writing {sha}, retrying")
                    if retries < MAX_RETRIES:
                        retries += 1
                    else:
                        raise

    def _download(
        self,
        input_queue: "Queue[Union[str, Poison]]",
        output_queue: "Queue[Tuple[str, Optional[List[str]]]]",
    ) -> None:
        """
        Download files given in input_queue and push results to output_queue.

        Results are tuples of (sha, list of objects referenced by the object),
        with None in place of the list if the download failed; the coordinator
        (`_fetch`) decides whether a failure is fatal.
        """
        while True:
            obj = input_queue.get()
            if isinstance(obj, Poison):
                return
            try:
                data, kind, contents = self._get_object(obj)
                git.write_loose_object(obj, data)
                output_queue.put((obj, git.referenced_objects_from_data(kind, contents)))
            except Exception as e:  # noqa: BLE001
                self._trace(f"error fetching object {obj}: {e}")
                output_queue.put((obj, None))

    def _list_remote_object_shas(self) -> List[str]:
        """
        Return the shas of all objects present on the remote.

        If a local Dropbox sync folder is available, list it instead of
        calling the API: listing directories does not materialize
        placeholder files, and the result may only lag behind the server,
        which is harmless — the object graph walk fetches anything newer.
        """
        local_dir = self._local_dir
        if local_dir is not None:
            shas = []
            objects_dir = os.path.join(local_dir, "objects")
            for prefix in os.listdir(objects_dir):
                subdir = os.path.join(objects_dir, prefix)
                if not os.path.isdir(subdir):
                    continue
                for suffix in os.listdir(subdir):
                    sha = prefix + suffix
                    if _is_sha(sha):
                        shas.append(sha)
            return shas
        loc = posixpath.join(self._path, "objects")
        try:
            res = self._connection.files_list_folder(loc, recursive=True)
            entries = res.entries
            while res.has_more:
                res = self._connection.files_list_folder_continue(res.cursor)
                entries.extend(res.entries)
        except dropbox.exceptions.ApiError as e:
            if not isinstance(e.error, dropbox.files.ListFolderError):
                raise
            return []  # empty repository
        shas = []
        for entry in entries:
            if not isinstance(entry, dropbox.files.FileMetadata):
                continue
            # objects are stored as <path>/objects/<sha prefix>/<sha suffix>
            prefix, suffix = entry.path_lower.split("/")[-2:]
            sha = prefix + suffix
            if _is_sha(sha):
                shas.append(sha)
        return shas

    def _fetch(self, sha: str) -> None:
        """
        Recursively fetch the given object and the objects it references.

        The object graph walk discovers a commit's parent only after
        downloading the commit, costing a network round trip per commit no
        matter how parallel the downloads are. To avoid this, when it is clear
        that a lot of data is missing — the local repository is empty (e.g. on
        clone), or the walk has already downloaded BULK_FETCH_THRESHOLD
        objects — all remote objects that are missing locally are enqueued for
        download, so that downloads proceed at full parallelism. This may
        download objects that are unreachable from any ref (git ignores such
        loose objects); download failures are only fatal for objects known to
        be needed — those reachable from the requested ref — so garbage on the
        remote (e.g. a partially-uploaded dangling commit from an aborted
        push, referencing objects that were never uploaded) cannot break the
        fetch. The walk always runs to completion and remains responsible for
        guaranteeing that everything reachable is present.
        """
        # have multiple threads downloading in parallel
        queue = [sha]
        pending: Set[str] = set()
        downloaded: Set[str] = set()
        needed: Set[str] = {sha}  # transitively referenced by the requested ref
        failed: Set[str] = set()  # failed downloads that were not needed (yet)
        input_queue: Queue[Union[str, Poison]] = Queue()  # requesting downloads
        output_queue: Queue[Tuple[str, Optional[List[str]]]] = Queue()  # completed downloads

        def bulk_enqueue() -> None:
            self._trace("bulk fetching all missing remote objects")
            queue.extend(
                obj
                for obj in self._list_remote_object_shas()
                if obj not in downloaded and obj not in pending and not git.object_exists(obj)
            )

        bulk_done = not git.repository_has_objects()
        if bulk_done:
            bulk_enqueue()
        procs = []
        for _ in range(self._processes):
            target = Binder(self, "_download")
            args = (input_queue, output_queue)
            # use multiprocessing.dummy to use threads instead of processes
            proc = multiprocessing.dummy.Process(target=target, args=args)
            proc.daemon = True
            proc.start()
            procs.append(proc)
        self._trace("", level=Level.INFO, exact=True)  # for showing progress
        done = total = 0
        while queue or pending:
            if queue:
                # if possible, queue up download
                sha = queue.pop()
                if sha in downloaded or sha in pending:
                    continue
                if git.object_exists(sha):
                    if sha == git.EMPTY_TREE_HASH:
                        # git.object_exists() returns True for the empty
                        # tree hash even if it's not present in the object
                        # store. Everything will work fine in this situation,
                        # but `git fsck` will complain if it's not present, so
                        # we explicitly add it to avoid that.
                        git.write_object("tree", b"")
                    if not git.history_exists(sha):
                        # this can only happen in the case of aborted fetches
                        # that are resumed later
                        self._trace(f"missing part of history from {sha}")
                        queue.extend(git.referenced_objects(sha))
                    else:
                        self._trace(f"{sha} already downloaded")
                else:
                    pending.add(sha)
                    input_queue.put(sha)
            else:
                # process completed download
                obj, referenced = output_queue.get()
                pending.remove(obj)
                if referenced is None:
                    # download failed (see _download for details)
                    if obj not in needed:
                        # the object is not (yet) known to be needed, so this
                        # is not fatal; if it becomes needed later, it is
                        # retried, and failure is fatal then
                        self._trace(f"skipping unneeded object {obj}")
                        failed.add(obj)
                        continue
                    self._fatal(f"failed to fetch {obj} (run with -v for details)")
                downloaded.add(obj)
                if obj in needed:
                    needed.update(referenced)
                    # retry previously-failed downloads that are now needed
                    for ref_sha in referenced:
                        if ref_sha in failed:
                            failed.discard(ref_sha)
                            queue.append(ref_sha)
                queue.extend(referenced)
                if not bulk_done and len(downloaded) >= BULK_FETCH_THRESHOLD:
                    # this fetch is large enough that listing the remote and
                    # downloading in bulk beats walking the object graph
                    bulk_done = True
                    bulk_enqueue()
                # show progress
                done = len(downloaded)
                total = done + len(pending)
                pct = int(float(done) / total * 100)
                message = f"\rReceiving objects: {pct:3.0f}% ({done}/{total})"
                self._trace(message, level=Level.INFO, exact=True)
        if total:
            self._trace(
                f"\rReceiving objects: 100% ({done}/{total}), done.\n",
                level=Level.INFO,
                exact=True,
            )
        for _ in procs:
            input_queue.put(Poison())
        for proc in procs:
            proc.join()

    def _write_ref(self, new_sha: str, dst: str, *, force: bool = False) -> Optional[str]:
        """
        Atomically update the given reference to point to the given object.

        Return None if there is no error, otherwise return a description of the
        error.
        """
        path = self._ref_path(dst)
        if force:
            # overwrite regardless of what is there before
            mode = dropbox.files.WriteMode.overwrite
        else:
            info = self._refs.get(dst, None)
            if info:
                rev, sha = info
                if not git.object_exists(sha):
                    return "fetch first"
                is_fast_forward = git.is_ancestor(sha, new_sha)
                if not is_fast_forward and not force:
                    return "non-fast forward"
                # perform an atomic compare-and-swap
                mode = dropbox.files.WriteMode.update(rev)
            else:
                # perform an atomic add, which fails if a concurrent writer
                # writes before this does
                mode = dropbox.files.WriteMode.add
        self._trace(f"writing ref {dst} with mode {mode}")
        data = f"{new_sha}\n".encode()
        try:
            self._connection.files_upload(data, path, mode, strict_conflict=True, mute=True)
        except dropbox.exceptions.ApiError as e:
            if not isinstance(e.error, dropbox.files.UploadError):
                raise
            return "fetch first"
        else:
            return None

    def get_refs(self, *, for_push: bool) -> List[Tuple[str, str]]:
        """
        Return the refs present on the remote.

        Return a list of tuples of (sha, name).
        """
        try:
            loc = posixpath.join(self._path, "refs")
            res = self._connection.files_list_folder(loc, recursive=True)
            files = res.entries
            while res.has_more:
                res = self._connection.files_list_folder_continue(res.cursor)
                files.extend(res.entries)
        except dropbox.exceptions.ApiError as e:
            if not isinstance(e.error, dropbox.files.ListFolderError):
                raise
            if not for_push:
                # if we're pushing, it's okay if nothing exists beforehand,
                # but it's good to notify the user just in case
                self._trace("repository is empty", Level.INFO)
            else:
                self._first_push = True
            return []
        files = [i for i in files if isinstance(i, dropbox.files.FileMetadata)]
        paths = [i.path_lower for i in files]
        if not paths:
            return []
        revs: List[str] = []
        data: List[bytes] = []
        for rev, datum in self._get_files(paths):
            revs.append(rev)
            data.append(datum)
        refs = []
        for path, rev, datum in zip(paths, revs, data):
            name = self._ref_name_from_path(path)
            sha = datum.decode("utf8").strip()
            self._refs[name] = (rev, sha)
            refs.append((sha, name))
        return refs

    def write_symbolic_ref(self, path: str, ref: str, rev: Optional[str] = None) -> bool:
        """
        Write the given symbolic ref to the remote.

        Perform a compare-and-swap (using previous revision rev) if specified,
        otherwise perform a regular write.

        Return a boolean indicating whether the write was successful.
        """
        path = posixpath.join(self._path, path)
        # choose between atomic compare-and-swap and atomic add
        mode = dropbox.files.WriteMode.update(rev) if rev else dropbox.files.WriteMode.add
        data = f"ref: {ref}\n".encode()
        self._trace(f"writing symbolic ref {path} with mode {mode}")
        try:
            self._connection.files_upload(data, path, mode, strict_conflict=True, mute=True)
        except dropbox.exceptions.ApiError as e:
            if not isinstance(e.error, dropbox.files.UploadError):
                raise
            return False
        return True

    def read_symbolic_ref(self, path: str) -> Optional[Tuple[str, str]]:
        """
        Return the revision number and content of a given symbolic ref on the remote.

        Return a tuple (revision, content), or None if the symbolic ref does not exist.
        """
        path = posixpath.join(self._path, path)
        self._trace(f"fetching symbolic ref: {path}")
        try:
            meta, resp = self._connection.files_download(path)
        except dropbox.exceptions.ApiError as e:
            if not isinstance(e.error, dropbox.files.DownloadError):
                raise
            return None
        ref = resp.content.decode("utf8")
        ref = ref[len("ref: ") :].rstrip()
        rev = meta.rev
        return (rev, ref)


def _is_sha(name: str) -> bool:
    """
    Return whether name looks like a full sha1 hex digest.
    """
    return len(name) == 40 and all(c in "0123456789abcdef" for c in name)  # noqa: PLR2004


def _write(message: Optional[str] = None) -> None:
    """
    Write a message to standard output.
    """
    if message is not None:
        stdout(f"{message}\n")
    else:
        stdout("\n")
