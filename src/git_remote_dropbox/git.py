import hashlib
import os
import subprocess
import threading
import zlib
from functools import lru_cache
from typing import List, Optional, Tuple

from git_remote_dropbox.constants import DEVNULL
from git_remote_dropbox.util import atomic_write

EMPTY_TREE_HASH: str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


class _CatFile:
    """
    A persistent `git cat-file --batch` (or `--batch-check`) process.

    Spawning a git subprocess per object dominates the runtime when
    pushing/fetching many objects; a single batch process serves all requests
    over a pipe instead. Instances are not thread-safe: use one per thread
    (see `_cat_file` / `_cat_file_check`).
    """

    def __init__(self, *, check: bool) -> None:
        flag = "--batch-check" if check else "--batch"
        proc = subprocess.Popen(
            ["git", "cat-file", flag],  # noqa: S607
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=DEVNULL,
        )
        if proc.stdin is None or proc.stdout is None:
            msg = "failed to open pipes to git cat-file"
            raise RuntimeError(msg)
        self._stdin = proc.stdin
        self._stdout = proc.stdout
        self._check = check

    def query(self, sha: str) -> Optional[Tuple[str, bytes]]:
        """
        Return (kind, contents) for the given object, or None if it is missing.

        In check mode, contents is always b"".
        """
        self._stdin.write(sha.encode("utf8") + b"\n")
        self._stdin.flush()
        header = self._stdout.readline().split()
        try:
            _, kind_bytes, size_bytes = header
        except ValueError:
            # "<sha> missing" (or "<sha> ambiguous")
            return None
        kind = kind_bytes.decode("utf8")
        size = int(size_bytes)
        if self._check:
            return (kind, b"")
        contents = self._stdout.read(size)
        self._stdout.readline()  # trailing newline after the contents
        return (kind, contents)


_per_thread = threading.local()


def _cat_file() -> _CatFile:
    proc: Optional[_CatFile] = getattr(_per_thread, "cat_file", None)
    if proc is None:
        proc = _CatFile(check=False)
        _per_thread.cat_file = proc
    return proc


def _cat_file_check() -> _CatFile:
    proc: Optional[_CatFile] = getattr(_per_thread, "cat_file_check", None)
    if proc is None:
        proc = _CatFile(check=True)
        _per_thread.cat_file_check = proc
    return proc


def command_output_raw(*args: str) -> bytes:
    """
    Return the raw result of running a git command.
    """
    args = ("git", *args)
    return subprocess.check_output(args, stderr=DEVNULL)


def command_output(*args: str) -> str:
    """
    Return the raw result of running a git command.
    """
    return command_output_raw(*args).decode("utf8").strip()


def command_ok(*args: str) -> bool:
    """
    Return whether a git command runs successfully.
    """
    args = ("git", *args)
    return subprocess.call(args, stdout=DEVNULL, stderr=DEVNULL) == 0


def is_ancestor(ancestor: str, ref: str) -> bool:
    """
    Return whether ancestor is an ancestor of ref.

    This returns true when it is possible to fast-forward from ancestor to ref.
    """
    return command_ok("merge-base", "--is-ancestor", ancestor, ref)


def object_exists(sha: str) -> bool:
    """
    Return whether the object exists in the repository.
    """
    return _cat_file_check().query(sha) is not None


def history_exists(sha: str) -> bool:
    """
    Return whether the object, along with its history, exists in the
    repository.
    """
    return command_ok("rev-list", "--objects", sha)


def ref_value(ref: str) -> str:
    """
    Return the hash of the ref.
    """
    return command_output("rev-parse", ref)


def symbolic_ref_value(name: str) -> str:
    """
    Return the branch head to which the symbolic ref refers.
    """
    return command_output("symbolic-ref", name)


def encode_object(sha: str) -> bytes:
    """
    Return the encoded contents of the object.

    The encoding is identical to the encoding git uses for loose objects.

    This operation is the inverse of `decode_object`.
    """
    res = _cat_file().query(sha)
    if res is None:
        msg = f"object not found: {sha}"
        raise ValueError(msg)
    kind, contents = res
    data = kind.encode("utf8") + b" " + str(len(contents)).encode("utf8") + b"\0" + contents
    return zlib.compress(data)


def decode_object(data: bytes) -> Tuple[str, str, bytes]:
    """
    Decode an encoded object without writing it.

    Return a tuple (computed sha, kind, contents).

    This operation is the inverse of `encode_object`.
    """
    decompressed = zlib.decompress(data)
    sha = hashlib.sha1(decompressed).hexdigest()  # noqa: S324
    header, contents = decompressed.split(b"\0", 1)
    kind = header.split()[0].decode("utf8")
    return (sha, kind, contents)


@lru_cache(maxsize=None)
def _objects_dir() -> str:
    return command_output("rev-parse", "--git-path", "objects")


def write_loose_object(sha: str, data: bytes) -> None:
    """
    Write an encoded object (as produced by `encode_object`) directly into the
    object store as a loose object.

    The caller is responsible for verifying that sha matches the data (see
    `decode_object`).
    """
    path = os.path.join(_objects_dir(), sha[:2], sha[2:])
    if os.path.exists(path):
        # objects are content-addressed, so an existing object needs no update
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    atomic_write(data, path)


def write_object(kind: str, contents: bytes) -> str:
    with subprocess.Popen(
        ["git", "hash-object", "-w", "--stdin", "-t", kind],  # noqa: S607
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=DEVNULL,
    ) as p:
        sha = p.communicate(contents)[0].decode("utf8").strip()
    return sha  # noqa: RET504


def list_objects(ref: str, exclude: List[str]) -> List[str]:
    """
    Return the objects reachable from ref excluding the objects reachable from
    exclude.
    """
    exclude = [f"^{obj}" for obj in exclude if object_exists(obj)]
    objects = command_output("rev-list", "--objects", ref, *exclude)
    if not objects:
        return []
    return [i.split()[0] for i in objects.split("\n")]


def referenced_objects(sha: str) -> List[str]:
    """
    Return the objects directly referenced by the object.
    """
    res = _cat_file().query(sha)
    if res is None:
        msg = f"object not found: {sha}"
        raise ValueError(msg)
    kind, contents = res
    return referenced_objects_from_data(kind, contents)


def referenced_objects_from_data(kind: str, contents: bytes) -> List[str]:
    """
    Return the objects directly referenced by an object, given its raw
    contents (as returned by `decode_object`).
    """
    if kind == "blob":
        # blob objects do not reference any other objects
        return []
    if kind == "tag":
        # tag objects reference a single object: the first header line is
        # "object <sha>"
        return [contents.split(b"\n", maxsplit=1)[0].split()[1].decode("utf8")]
    if kind == "commit":
        # commit objects reference a tree and zero or more parents
        objs = []
        for line in contents.split(b"\n"):
            if line.startswith((b"tree ", b"parent ")):
                objs.append(line.split()[1].decode("utf8"))
            elif not line.startswith(b" "):
                # end of the tree/parent headers (which always come first);
                # lines starting with a space are continuations of multi-line
                # headers such as gpgsig
                break
        return objs
    if kind == "tree":
        # tree objects reference zero or more trees and blobs, or submodules;
        # entries are "<mode> <name>\0" followed by a 20-byte binary sha
        objs = []
        i = 0
        while i < len(contents):
            mode_end = contents.index(b" ", i)
            mode = contents[i:mode_end]
            name_end = contents.index(b"\0", mode_end)
            sha_end = name_end + 21
            # submodules have the mode '160000', we filter them out because
            # there is nothing to download and this causes errors
            if mode != b"160000":
                objs.append(contents[name_end + 1 : sha_end].hex())
            i = sha_end
        return objs
    msg = f"unexpected git object type: {kind}"
    raise ValueError(msg)


def repository_has_objects() -> bool:
    """
    Return whether the local repository contains any objects.
    """
    counts = {}
    for line in command_output("count-objects", "-v").splitlines():
        key, _, value = line.partition(": ")
        counts[key] = value
    return int(counts["count"]) > 0 or int(counts["in-pack"]) > 0


def get_remote_url(name: str) -> str:
    """
    Return the URL of the given remote.
    """
    return command_output("remote", "get-url", name)
