"""Download and load TarKG data files.

TarKG publishes KG, entity-information, and feature CSV files at:
https://tarkg.ddtmlab.org/download

The downloader caches files under ``~/.datasets/tarkg_data`` by default and
skips files that are already present unless ``force=True`` is used.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal
from urllib.error import HTTPError
from urllib.parse import urljoin
from urllib.request import Request, urlopen
import html
import hashlib
import os
import ssl
import sys
import threading
import time


BASE_URL = "https://tarkg.ddtmlab.org"
DEFAULT_CACHE_DIR = Path("~/.datasets/tarkg_data").expanduser()
CHUNK_SIZE = 1024 * 1024

Category = Literal["kg", "entity", "relation", "feature"]
Selector = bool | str | Iterable[str]


@dataclass(frozen=True)
class TarKGFile:
    """Metadata for a downloadable TarKG file."""

    name: str
    category: Category
    description: str
    download_size: str
    path: str

    @property
    def url(self) -> str:
        return urljoin(BASE_URL, self.path)

    @property
    def stem(self) -> str:
        return self.name.removesuffix(".csv").removesuffix(".md")


FILES: tuple[TarKGFile, ...] = (
    TarKGFile(
        "README.md",
        "kg",
        "Introducing the data structure and content for download",
        "4KB",
        "/download/README.md",
    ),
    TarKGFile(
        "TarKG_nodes.csv",
        "kg",
        "Basic information for total unique nodes",
        "63M",
        "/download/KG_Data/TarKG_nodes.csv",
    ),
    TarKGFile(
        "TarKG_nodes_mapping.csv",
        "kg",
        "Basic information and data source for total nodes",
        "142M",
        "/download/KG_Data/TarKG_nodes_mapping.csv",
    ),
    TarKGFile(
        "TarKG_edges.csv",
        "relation",
        "Basic information for total unique edges",
        "1.7G",
        "/download/KG_Data/TarKG_edges.csv",
    ),
    TarKGFile(
        "TarKG_edges_mapping.csv",
        "relation",
        "Basic information and data source for total unique edges",
        "3.6G",
        "/download/KG_Data/TarKG_edges_mapping.csv",
    ),
    TarKGFile(
        "Compound_nodes.csv",
        "entity",
        "Compound entity information",
        "371M",
        "/download/Entity_Information/Compound_nodes.csv",
    ),
    TarKGFile(
        "Gene_nodes.csv",
        "entity",
        "Gene entity information",
        "223M",
        "/download/Entity_Information/Gene_nodes.csv",
    ),
    TarKGFile(
        "Disease_nodes.csv",
        "entity",
        "Disease entity information",
        "9.8M",
        "/download/Entity_Information/Disease_nodes.csv",
    ),
    TarKGFile(
        "Pathway_nodes.csv",
        "entity",
        "Pathway entity information",
        "3.5M",
        "/download/Entity_Information/Pathway_nodes.csv",
    ),
    TarKGFile(
        "Go_nodes.csv",
        "entity",
        "Gene Ontology entity information",
        "24M",
        "/download/Entity_Information/Go_nodes.csv",
    ),
    TarKGFile(
        "Anatomy_nodes.csv",
        "entity",
        "Anatomy entity information",
        "3.1M",
        "/download/Entity_Information/Anatomy_nodes.csv",
    ),
    TarKGFile(
        "Phenotype_nodes.csv",
        "entity",
        "Phenotype entity information",
        "2.7M",
        "/download/Entity_Information/Phenotype_nodes.csv",
    ),
    TarKGFile(
        "Side_Effect_nodes.csv",
        "entity",
        "Side effect entity information",
        "1.4M",
        "/download/Entity_Information/Side_Effect_nodes.csv",
    ),
    TarKGFile(
        "TCM_CMM_nodes.csv",
        "entity",
        "TCM_CMM entity information",
        "1.1M",
        "/download/Entity_Information/TCM_CMM_nodes.csv",
    ),
    TarKGFile(
        "TCM_Prescription_nodes.csv",
        "entity",
        "TCM prescription entity information",
        "703K",
        "/download/Entity_Information/TCM_Prescription_nodes.csv",
    ),
    TarKGFile(
        "TCM_Symptom_nodes.csv",
        "entity",
        "TCM symptom entity information",
        "482K",
        "/download/Entity_Information/TCM_Symptom_nodes.csv",
    ),
    TarKGFile(
        "Symptom_nodes.csv",
        "entity",
        "Symptom entity information",
        "146K",
        "/download/Entity_Information/Symptom_nodes.csv",
    ),
    TarKGFile(
        "TCM_Syndrome_nodes.csv",
        "entity",
        "TCM syndrome entity information",
        "52K",
        "/download/Entity_Information/TCM_Syndrome_nodes.csv",
    ),
    TarKGFile(
        "Compound_feature.csv",
        "feature",
        "Compound structure features",
        "180M",
        "/download/Entity_Feature/Compound_feature.csv",
    ),
    TarKGFile(
        "Disease_feature.csv",
        "feature",
        "Disease text features",
        "8.1M",
        "/download/Entity_Feature/Disease_feature.csv",
    ),
    TarKGFile(
        "Drug_feature.csv",
        "feature",
        "Drug structure features",
        "6.3M",
        "/download/Entity_Feature/Drug_feature.csv",
    ),
    TarKGFile(
        "Gene_feature.csv",
        "feature",
        "Gene sequence features",
        "83M",
        "/download/Entity_Feature/Gene_feature.csv",
    ),
)

# Hard-coded integrity hashes for TarKG files. Add the remaining file hashes
# after computing them from a trusted complete local cache with
# TarKGLoader.compute_cached_sha256(only_known=False).
SHA256_HASHES: dict[str, str] = {
    'Anatomy_nodes.csv': '896302a89b892207a6e937374b615f0f9f21a655f6fe23e81152c6396260dbb8',
    'Compound_feature.csv': 'a1a03dccc9085147d5c523f9a949572f1e312b5b789ed21656a4b085fe61f68a',
    'Compound_nodes.csv': '50d1cb50b8a344dfc3eac81a532b721bcaaaac9443df2d786f9d4f08eaf24525',
    'Disease_feature.csv': '9ed07e4a15ad9dde222a05bc314cce433ccc1e55d62358ab85102181a8dbf134',
    'Disease_nodes.csv': 'cee3467eae1b52ebe8390e4aae4f5a236b7d13675bde6cb282d5386b579697ce',
    'Drug_feature.csv': '296af7098c704ac2d01293a1fb1a461e28b349f8f79207ff60e7356b17c56266',
    'Gene_feature.csv': 'c5b4c096fe63207b5daef01d14428482a17f87d1cf8bfed31b4598a97459b23c',
    'Gene_nodes.csv': '675b13336c35edb96690e56c8102a25e3f5c8fe6acd3f81a95e13f558e687565',
    'Go_nodes.csv': '5e1542107633be2dd5972780fad53d7d51c201befbf0f1a603eaf37cdcff95af',
    'Pathway_nodes.csv': '683c87c7a3426a5f80be9ab3e28b7ecbd3fa64e9cf05d4809289e474e357a68f',
    'Phenotype_nodes.csv': '3596b8a1d14ed21c5458309356aa7c4686ae4a9ef1b04627ec097801b002626d',
    'README.md': 'd9ec9a3994cbb06b0928513a7a8304bb413d84051a00205098c1990d2adbe2df',
    'Side_Effect_nodes.csv': '6eb99d9aa6deaeae23268aff5f91b0f9cf2fbc210dabd164bdbaa69ed2886725',
    'Symptom_nodes.csv': 'aa8a21d8f961ddf51b29671746fa491368dc57b815e6316cef2900023954d4a4',
    'TCM_CMM_nodes.csv': 'bd8333be70235b5514beff3965a87a95107b28ccc98601d13be9a6e3fd806592',
    'TCM_Prescription_nodes.csv': '074edca6077ae591659b0d4e43eea47183aae9cc33901659fa59b32f121b84e4',
    'TCM_Symptom_nodes.csv': '43c94ec74143359ae066fb698fcecc33f968b976b62c60ea700f7eb7e131f652',
    'TCM_Syndrome_nodes.csv': 'bbd80cc218767d0e6b5df54e8fdde789e00f3d9c5eff722db037283d61a5d3ba',
    'TarKG_edges.csv': '634e95dcdd2de4790ca8d98712f0f79985443ebafc88b58a846a9c8fe075bad8',
    'TarKG_edges_mapping.csv': '1c3413cdab3c023753dea38f1e0321240a1b5796bf37922348ac9538e37c9425',
    'TarKG_nodes.csv': '9e0e3743507c562c7e97d676a7f5227202ed18e1ae8de960cdec61edfea6f9d6',
    'TarKG_nodes_mapping.csv': '9dbe14865e0af93a6254ab3c0462a91e75ed29d493105c67053deb34fee3f429'
}



class TarKGLoader:
    """Loader and cache manager for the TarKG knowledge graph dataset.

    The loader knows the file manifest published at
    ``https://tarkg.ddtmlab.org/download`` and provides a single interface for
    selecting dataset subsets, downloading them in parallel, resuming partial
    downloads, reusing cached files, and reading cached CSV files into pandas
    DataFrames. Files are cached under ``~/.datasets/tarkg_data`` by default,
    but the cache directory, download parallelism, progress reporting, and
    integrity checking behavior are configurable at construction time. HTTPS
    certificate validation is disabled by default because the TarKG host
    currently serves an expired TLS certificate.
    """

    def __init__(
        self,
        cache_dir: str | os.PathLike[str] = DEFAULT_CACHE_DIR,
        *,
        max_workers: int = 4,
        verify_ssl: bool = False,
        show_progress: bool = True,
        verify_hashes: bool = True,
    ) -> None:
        """Initialize a TarKG loader.

        Args:
            cache_dir: Directory where downloaded TarKG files are cached.
            max_workers: Maximum number of files to download concurrently.
            verify_ssl: Whether to verify HTTPS certificates. The TarKG host has
                previously served an expired certificate, so this defaults to
                ``False``.
            show_progress: Whether to show progress bars or progress messages
                during downloads.
            verify_hashes: Whether to validate cached and newly downloaded files
                against hard-coded SHA256 hashes when a hash is available.
        """

        self.cache_dir = Path(cache_dir).expanduser()
        self.max_workers = max(1, max_workers)
        self.verify_ssl = verify_ssl
        self.show_progress = show_progress
        self.verify_hashes = verify_hashes
        self._progress_display = None
        self._progress_lock = threading.Lock()
        self._progress_state = {}
        self._last_progress_render = 0.0

    def available_files(self) -> tuple[TarKGFile, ...]:
        """Return the static TarKG download manifest.

        Returns:
            A tuple of ``TarKGFile`` entries describing all known downloadable
            TarKG files.
        """

        return FILES

    def expected_sha256(self) -> dict[str, str]:
        """Return hard-coded SHA256 hashes for known TarKG files.

        Returns:
            A copy of the mapping from TarKG file names to expected SHA256
            hex digests. Files absent from this mapping are downloaded and
            cached, but cannot be integrity-checked until their hashes are added.
        """

        return dict(SHA256_HASHES)

    def select_files(
        self,
        *,
        kg: Selector = True,
        entities: Selector = True,
        relations: Selector = True,
        features: Selector = True,
    ) -> list[TarKGFile]:
        """Select files from the TarKG manifest.

        Args:
            kg: KG-level file selector. ``True`` selects all KG-level files,
                ``False`` selects none, and strings or iterables select by file
                name, stem, or prefix.
            entities: Entity-information file selector. Examples include
                ``"Gene"`` or ``["Compound", "Disease"]``.
            relations: Relation/edge file selector. Examples include
                ``"edges"`` or ``"edges_mapping"``.
            features: Entity-feature file selector. Examples include
                ``"Drug"`` or ``["Gene_feature.csv"]``.

        Returns:
            A list of manifest entries matching the requested selectors.
        """

        selected: list[TarKGFile] = []
        selectors = {
            "kg": kg,
            "entity": entities,
            "relation": relations,
            "feature": features,
        }

        for file in FILES:
            selector = selectors[file.category]
            if selector is True or self._matches_selector(file, selector):
                selected.append(file)

        return selected

    def download(
        self,
        *,
        kg: Selector = True,
        entities: Selector = True,
        relations: Selector = True,
        features: Selector = True,
        force: bool = False,
    ) -> dict[str, Path]:
        """Download selected TarKG files into the cache.

        Existing non-empty cached files are reused unless ``force=True`` is
        passed. Interrupted downloads are resumed from ``*.part`` files when the
        server supports HTTP range requests. Downloads run in parallel according
        to ``self.max_workers``.

        Args:
            kg: KG-level file selector.
            entities: Entity-information file selector.
            relations: Relation/edge file selector.
            features: Entity-feature file selector.
            force: Whether to overwrite files that are already cached.

        Returns:
            A dictionary mapping downloaded or reused file names to local cache
            paths.
        """

        files = self.select_files(
            kg=kg,
            entities=entities,
            relations=relations,
            features=features,
        )
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        if not files:
            return {}

        results: dict[str, Path] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._download_one,
                    file,
                    force=force,
                    progress_position=position % self.max_workers,
                ): file
                for position, file in enumerate(files)
            }
            for future in as_completed(futures):
                file = futures[future]
                results[file.name] = future.result()

        return dict(sorted(results.items()))

    def load(
        self,
        *,
        download: bool = True,
        kg: Selector = True,
        entities: Selector = True,
        relations: Selector = True,
        features: Selector = True,
        read_csv_kwargs: dict | None = None,
    ):
        """Load selected TarKG CSV files as pandas DataFrames.

        Args:
            download: Whether to download missing files before loading. Set this
                to ``False`` to read only from the current cache.
            kg: KG-level file selector.
            entities: Entity-information file selector.
            relations: Relation/edge file selector.
            features: Entity-feature file selector.
            read_csv_kwargs: Optional keyword arguments forwarded to
                ``pandas.read_csv``.

        Returns:
            A dictionary mapping CSV stems, such as ``"Gene_nodes"``, to pandas
            DataFrames. Non-CSV files such as ``README.md`` are skipped.

        Raises:
            ImportError: If pandas is not installed.
            FileNotFoundError: If ``download=False`` and any selected CSV file is
                missing from the cache.
        """

        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError(
                "TarKGLoader.load requires pandas. Use TarKGLoader.download "
                "for download-only usage."
            ) from exc

        files = self.select_files(
            kg=kg,
            entities=entities,
            relations=relations,
            features=features,
        )

        if download:
            self.download(
                kg=kg,
                entities=entities,
                relations=relations,
                features=features,
            )

        kwargs = read_csv_kwargs or {}
        data = {}
        missing = []
        for file in files:
            if not file.name.endswith(".csv"):
                continue
            path = self.cache_dir / file.name
            if path.exists():
                data[file.stem] = pd.read_csv(path, **kwargs)
            else:
                missing.append(path)

        if missing:
            missing_paths = ", ".join(str(path) for path in missing)
            raise FileNotFoundError(f"Cached TarKG files not found: {missing_paths}")

        return data

    def cached_paths(
        self,
        *,
        kg: Selector = True,
        entities: Selector = True,
        relations: Selector = True,
        features: Selector = True,
        only_existing: bool = True,
    ) -> dict[str, Path]:
        """Return cache paths for selected TarKG files.

        Args:
            kg: KG-level file selector.
            entities: Entity-information file selector.
            relations: Relation/edge file selector.
            features: Entity-feature file selector.
            only_existing: Whether to omit paths for files not currently cached.

        Returns:
            A dictionary mapping selected file names to cache paths.
        """

        paths = {
            file.name: self.cache_dir / file.name
            for file in self.select_files(
                kg=kg,
                entities=entities,
                relations=relations,
                features=features,
            )
        }
        if only_existing:
            paths = {name: path for name, path in paths.items() if path.exists()}
        return paths

    def sha256(self, path: str | os.PathLike[str]) -> str:
        """Compute the SHA256 digest for a local file.

        Args:
            path: File path whose digest should be computed.

        Returns:
            The SHA256 hex digest of the file contents.
        """

        digest = hashlib.sha256()
        with Path(path).open("rb") as file_obj:
            while chunk := file_obj.read(CHUNK_SIZE):
                digest.update(chunk)
        return digest.hexdigest()

    def compute_cached_sha256(
        self,
        *,
        kg: Selector = True,
        entities: Selector = True,
        relations: Selector = True,
        features: Selector = True,
        only_existing: bool = True,
        only_known: bool = False,
    ) -> dict[str, str]:
        """Compute SHA256 hashes for selected cached files.

        Args:
            kg: KG-level file selector.
            entities: Entity-information file selector.
            relations: Relation/edge file selector.
            features: Entity-feature file selector.
            only_existing: Whether to skip selected files that are not cached.
            only_known: Whether to restrict computation to files already present
                in the hard-coded ``SHA256_HASHES`` mapping.

        Returns:
            A dictionary mapping file names to computed SHA256 hex digests.

        Raises:
            FileNotFoundError: If ``only_existing=False`` and a selected file is
                missing from the cache.
        """

        paths = self.cached_paths(
            kg=kg,
            entities=entities,
            relations=relations,
            features=features,
            only_existing=only_existing,
        )
        if only_known:
            paths = {
                name: path for name, path in paths.items() if name in SHA256_HASHES
            }

        hashes = {}
        for name, path in paths.items():
            if not path.exists():
                raise FileNotFoundError(f"Cached TarKG file not found: {path}")
            hashes[name] = self.sha256(path)
        return dict(sorted(hashes.items()))

    def verify_cached_files(
        self,
        *,
        kg: Selector = True,
        entities: Selector = True,
        relations: Selector = True,
        features: Selector = True,
    ) -> dict[str, Path]:
        """Verify selected cached files against known SHA256 hashes.

        Files without hard-coded hashes are ignored.

        Args:
            kg: KG-level file selector.
            entities: Entity-information file selector.
            relations: Relation/edge file selector.
            features: Entity-feature file selector.

        Returns:
            A dictionary mapping verified file names to their cache paths.

        Raises:
            FileNotFoundError: If a selected file with a known hash is missing.
            ValueError: If a cached file's SHA256 digest differs from the
                hard-coded expected digest.
        """

        verified = {}
        for file in self.select_files(
            kg=kg,
            entities=entities,
            relations=relations,
            features=features,
        ):
            if file.name not in SHA256_HASHES:
                continue
            path = self.cache_dir / file.name
            self._verify_sha256(file, path)
            verified[file.name] = path
        return verified

    def _download_one(
        self,
        file: TarKGFile,
        *,
        force: bool,
        progress_position: int,
    ) -> Path:
        """Download one manifest entry to the cache.

        Args:
            file: Manifest entry to download.
            force: Whether to overwrite an existing cached file.
            progress_position: Vertical progress-bar position used by ``tqdm``
                when several files download in parallel.

        Returns:
            The local cached path for the file.
        """

        destination = self.cache_dir / file.name
        if destination.exists() and destination.stat().st_size > 0 and not force:
            if self.verify_hashes:
                self._verify_sha256(file, destination)
            self._print_progress(f"cached {file.name}")
            return destination

        temp_destination = destination.with_suffix(destination.suffix + ".part")
        if force and temp_destination.exists():
            temp_destination.unlink()

        resume_from = temp_destination.stat().st_size if temp_destination.exists() else 0
        context = None if self.verify_ssl else ssl._create_unverified_context()
        headers = {"User-Agent": "tar-kg-loader/1.0"}
        if resume_from:
            headers["Range"] = f"bytes={resume_from}-"
            self._print_progress(
                f"resuming {file.name} from {self._format_bytes(resume_from)}"
            )
        request = Request(file.url, headers=headers)

        try:
            response = urlopen(request, context=context)
        except HTTPError as exc:
            if exc.code != 416 or not resume_from:
                raise
            # The partial file is larger than the remote range; restart safely.
            temp_destination.unlink()
            resume_from = 0
            request = Request(file.url, headers={"User-Agent": "tar-kg-loader/1.0"})
            response = urlopen(request, context=context)

        with response:
            status = response.getcode()
            if resume_from and status != 206:
                self._print_progress(f"server did not resume {file.name}; restarting")
                resume_from = 0

            total = self._response_total_size(response, resume_from)
            mode = "ab" if resume_from else "wb"
            self._stream_download(
                response,
                temp_destination,
                file.name,
                total,
                initial=resume_from,
                mode=mode,
                progress_position=progress_position,
            )

        temp_destination.replace(destination)
        if self.verify_hashes:
            self._verify_sha256(file, destination)
        return destination

    def _response_total_size(self, response, initial: int) -> int:
        """Infer the full response size for progress reporting.

        Args:
            response: Open HTTP response returned by ``urllib.request.urlopen``.
            initial: Number of bytes already present in a resumed partial file.

        Returns:
            Total file size in bytes, or ``0`` when the server did not provide a
            usable size.
        """

        content_range = response.headers.get("Content-Range")
        if content_range and "/" in content_range:
            total = content_range.rsplit("/", maxsplit=1)[-1]
            if total.isdigit():
                return int(total)

        content_length = int(response.headers.get("Content-Length") or 0)
        if response.getcode() == 206:
            return initial + content_length
        return content_length

    def _verify_sha256(self, file: TarKGFile, path: Path) -> None:
        """Verify a file against the hard-coded SHA256 mapping when available.

        Args:
            file: Manifest entry associated with the local file.
            path: Local cached path to verify.

        Raises:
            FileNotFoundError: If the local file does not exist.
            ValueError: If a known expected hash does not match the actual hash.
        """

        expected = SHA256_HASHES.get(file.name)
        if expected is None:
            return
        if not path.exists():
            raise FileNotFoundError(f"Cached TarKG file not found: {path}")

        actual = self.sha256(path)
        if actual != expected:
            raise ValueError(
                f"SHA256 mismatch for {file.name}: expected {expected}, got {actual}"
            )

    def _stream_download(
        self,
        response,
        destination: Path,
        label: str,
        total: int,
        *,
        initial: int,
        mode: str,
        progress_position: int,
    ) -> None:
        """Stream an HTTP response body to disk.

        Args:
            response: Open HTTP response returned by ``urllib.request.urlopen``.
            destination: Temporary file path to write.
            label: Label to display in progress output.
            total: Expected response size in bytes, or ``0`` if unknown.
            initial: Number of bytes already downloaded before this response.
            mode: File write mode, usually ``"wb"`` or ``"ab"``.
            progress_position: Vertical progress-bar position used by ``tqdm``
                when several files download in parallel.
        """

        use_notebook_progress = self.show_progress and self._is_notebook()
        tqdm = None if use_notebook_progress else self._tqdm()

        downloaded = initial
        with destination.open(mode) as output:
            if self.show_progress and tqdm is not None:
                with tqdm(
                    total=total or None,
                    initial=initial,
                    unit="B",
                    unit_scale=True,
                    desc=label,
                    position=progress_position,
                    leave=False,
                    dynamic_ncols=True,
                ) as bar:
                    while chunk := response.read(CHUNK_SIZE):
                        output.write(chunk)
                        bar.update(len(chunk))
                return

            if use_notebook_progress:
                self._update_notebook_progress(
                    label,
                    downloaded,
                    total,
                    progress_position,
                )
                while chunk := response.read(CHUNK_SIZE):
                    output.write(chunk)
                    downloaded += len(chunk)
                    self._update_notebook_progress(
                        label,
                        downloaded,
                        total,
                        progress_position,
                    )
                self._update_notebook_progress(
                    label,
                    downloaded,
                    total,
                    progress_position,
                    done=True,
                )
                return

            while chunk := response.read(CHUNK_SIZE):
                output.write(chunk)
                downloaded += len(chunk)

        if self.show_progress and tqdm is None:
            size = self._format_bytes(downloaded)
            self._print_progress(f"downloaded {label} ({size})")

    def _tqdm(self):
        """Return the best available tqdm progress-bar implementation.

        Returns:
            ``tqdm.tqdm`` when tqdm is installed, otherwise ``None``. Notebook
            progress uses a custom HTML display to avoid ANSI escape-code spam
            when Jupyter widget support is unavailable.
        """

        try:
            from tqdm import tqdm
        except ImportError:
            return None
        return tqdm

    def _is_notebook(self) -> bool:
        """Return whether the loader is running inside a Jupyter notebook.

        Returns:
            ``True`` when the active IPython shell is a Jupyter kernel.
        """

        try:
            from IPython import get_ipython
        except ImportError:
            return False

        shell = get_ipython()
        if shell is None:
            return False
        return shell.__class__.__name__ == "ZMQInteractiveShell"

    def _update_notebook_progress(
        self,
        label: str,
        downloaded: int,
        total: int,
        position: int,
        *,
        done: bool = False,
    ) -> None:
        """Render or update the notebook HTML progress table.

        Args:
            label: Downloaded file name.
            downloaded: Number of bytes downloaded so far.
            total: Total file size in bytes, or ``0`` when unknown.
            position: Stable row ordering key for parallel downloads.
            done: Whether the file has completed downloading.
        """

        now = time.monotonic()
        with self._progress_lock:
            self._progress_state[label] = {
                "downloaded": downloaded,
                "total": total,
                "position": position,
                "done": done,
            }
            if not done and now - self._last_progress_render < 0.5:
                return
            self._last_progress_render = now
            rendered = self._render_notebook_progress()

            try:
                from IPython.display import HTML, display
            except ImportError:
                return

            if self._progress_display is None:
                self._progress_display = display(HTML(rendered), display_id=True)
            else:
                self._progress_display.update(HTML(rendered))

    def _render_notebook_progress(self) -> str:
        """Build the notebook HTML progress table.

        Returns:
            HTML markup for all active and completed downloads.
        """

        rows = []
        for label, state in sorted(
            self._progress_state.items(),
            key=lambda item: (item[1]["position"], item[0]),
        ):
            downloaded = state["downloaded"]
            total = state["total"]
            done = state["done"]
            percent = (downloaded / total * 100) if total else 0.0
            status = "done" if done else "downloading"
            total_text = self._format_bytes(total) if total else "unknown"
            rows.append(
                "<tr>"
                f"<td style='padding:2px 8px'>{html.escape(label)}</td>"
                "<td style='padding:2px 8px; min-width:220px'>"
                f"<progress value='{downloaded}' max='{total or downloaded or 1}' "
                "style='width:220px'></progress>"
                "</td>"
                f"<td style='padding:2px 8px'>{percent:5.1f}%</td>"
                f"<td style='padding:2px 8px'>{self._format_bytes(downloaded)} / {total_text}</td>"
                f"<td style='padding:2px 8px'>{status}</td>"
                "</tr>"
            )

        return (
            "<table style='font-family:monospace; border-collapse:collapse'>"
            "<thead><tr>"
            "<th style='text-align:left; padding:2px 8px'>file</th>"
            "<th style='text-align:left; padding:2px 8px'>progress</th>"
            "<th style='text-align:left; padding:2px 8px'>%</th>"
            "<th style='text-align:left; padding:2px 8px'>downloaded</th>"
            "<th style='text-align:left; padding:2px 8px'>status</th>"
            "</tr></thead><tbody>"
            + "".join(rows)
            + "</tbody></table>"
        )

    def _matches_selector(self, file: TarKGFile, selector: Selector) -> bool:
        """Return whether a manifest file matches a selector.

        Args:
            file: Manifest entry to test.
            selector: Selector string, iterable of strings, ``False``, or
                ``None``.

        Returns:
            ``True`` when the selector matches one of the file aliases.
        """

        if selector is False or selector is None:
            return False

        selected = [selector] if isinstance(selector, str) else selector
        wanted = {self._normalize(item) for item in selected}
        aliases = {
            self._normalize(file.name),
            self._normalize(file.stem),
            self._normalize(file.stem.removesuffix("_nodes")),
            self._normalize(file.stem.removesuffix("_feature")),
            self._normalize(file.stem.removeprefix("TarKG_")),
        }
        return bool(wanted & aliases)

    def _normalize(self, value: object) -> str:
        """Normalize selector values before matching.

        Args:
            value: Selector or alias value.

        Returns:
            Lowercase string with spaces and hyphens converted to underscores.
        """

        return str(value).lower().replace("-", "_").replace(" ", "_")

    def _format_bytes(self, size: int) -> str:
        """Format a byte count for progress messages.

        Args:
            size: Number of bytes.

        Returns:
            Human-readable size string.
        """

        value = float(size)
        for unit in ("B", "KB", "MB", "GB"):
            if value < 1024 or unit == "GB":
                return f"{value:.1f}{unit}"
            value /= 1024
        return f"{value:.1f}GB"

    def _print_progress(self, message: str) -> None:
        """Print a progress message when progress output is enabled.

        Args:
            message: Message to print to standard error.
        """

        if self.show_progress:
            print(message, file=sys.stderr)


def available_files() -> tuple[TarKGFile, ...]:
    """Return the static TarKG download manifest.

    Returns:
        A tuple of ``TarKGFile`` entries describing all known downloadable TarKG
        files.
    """

    return TarKGLoader().available_files()


def select_files(
    *,
    kg: Selector = True,
    entities: Selector = True,
    relations: Selector = True,
    features: Selector = True,
) -> list[TarKGFile]:
    """Select files from the TarKG manifest.

    Args:
        kg: KG-level file selector.
        entities: Entity-information file selector.
        relations: Relation/edge file selector.
        features: Entity-feature file selector.

    Returns:
        A list of manifest entries matching the requested selectors.
    """

    return TarKGLoader().select_files(
        kg=kg,
        entities=entities,
        relations=relations,
        features=features,
    )


def download_tarkg(
    *,
    cache_dir: str | os.PathLike[str] = DEFAULT_CACHE_DIR,
    max_workers: int = 4,
    kg: Selector = True,
    entities: Selector = True,
    relations: Selector = True,
    features: Selector = True,
    force: bool = False,
    verify_ssl: bool = False,
    show_progress: bool = True,
    verify_hashes: bool = True,
) -> dict[str, Path]:
    """Download selected TarKG files into the local cache.

    Args:
        cache_dir: Directory where downloaded files are cached.
        max_workers: Maximum number of files to download concurrently.
        kg: KG-level file selector.
        entities: Entity-information file selector.
        relations: Relation/edge file selector.
        features: Entity-feature file selector.
        force: Whether to overwrite files that are already cached.
        verify_ssl: Whether to verify HTTPS certificates. Defaults to ``False``
            because the TarKG server currently has an expired TLS certificate.
        show_progress: Whether to show progress bars or messages.
        verify_hashes: Whether to validate files against known hard-coded SHA256
            hashes after download or cache reuse.

    Returns:
        A dictionary mapping downloaded or reused file names to cache paths.
    """

    return TarKGLoader(
        cache_dir=cache_dir,
        max_workers=max_workers,
        verify_ssl=verify_ssl,
        show_progress=show_progress,
        verify_hashes=verify_hashes,
    ).download(
        kg=kg,
        entities=entities,
        relations=relations,
        features=features,
        force=force,
    )


def load_tarkg(
    *,
    cache_dir: str | os.PathLike[str] = DEFAULT_CACHE_DIR,
    download: bool = True,
    max_workers: int = 4,
    kg: Selector = True,
    entities: Selector = True,
    relations: Selector = True,
    features: Selector = True,
    verify_ssl: bool = False,
    show_progress: bool = True,
    verify_hashes: bool = True,
    read_csv_kwargs: dict | None = None,
):
    """Load selected TarKG CSV files as pandas DataFrames.

    Args:
        cache_dir: Directory where downloaded files are cached.
        download: Whether to download missing files before loading.
        max_workers: Maximum number of files to download concurrently.
        kg: KG-level file selector.
        entities: Entity-information file selector.
        relations: Relation/edge file selector.
        features: Entity-feature file selector.
        verify_ssl: Whether to verify HTTPS certificates. Defaults to ``False``
            because the TarKG server currently has an expired TLS certificate.
        show_progress: Whether to show progress bars or messages.
        verify_hashes: Whether to validate files against known hard-coded SHA256
            hashes after download or cache reuse.
        read_csv_kwargs: Optional keyword arguments forwarded to
            ``pandas.read_csv``.

    Returns:
        A dictionary mapping CSV stems to pandas DataFrames.
    """

    return TarKGLoader(
        cache_dir=cache_dir,
        max_workers=max_workers,
        verify_ssl=verify_ssl,
        show_progress=show_progress,
        verify_hashes=verify_hashes,
    ).load(
        download=download,
        kg=kg,
        entities=entities,
        relations=relations,
        features=features,
        read_csv_kwargs=read_csv_kwargs,
    )


def cached_paths(
    *,
    cache_dir: str | os.PathLike[str] = DEFAULT_CACHE_DIR,
    kg: Selector = True,
    entities: Selector = True,
    relations: Selector = True,
    features: Selector = True,
    only_existing: bool = True,
) -> dict[str, Path]:
    """Return cache paths for selected TarKG files.

    Args:
        cache_dir: Directory where downloaded files are cached.
        kg: KG-level file selector.
        entities: Entity-information file selector.
        relations: Relation/edge file selector.
        features: Entity-feature file selector.
        only_existing: Whether to omit paths for files not currently cached.

    Returns:
        A dictionary mapping selected file names to cache paths.
    """

    return TarKGLoader(cache_dir=cache_dir).cached_paths(
        kg=kg,
        entities=entities,
        relations=relations,
        features=features,
        only_existing=only_existing,
    )


def expected_sha256() -> dict[str, str]:
    """Return hard-coded SHA256 hashes for known TarKG files.

    Returns:
        A copy of the mapping from TarKG file names to expected SHA256 digests.
    """

    return TarKGLoader().expected_sha256()


def compute_cached_sha256(
    *,
    cache_dir: str | os.PathLike[str] = DEFAULT_CACHE_DIR,
    kg: Selector = True,
    entities: Selector = True,
    relations: Selector = True,
    features: Selector = True,
    only_existing: bool = True,
    only_known: bool = False,
) -> dict[str, str]:
    """Compute SHA256 hashes for selected cached TarKG files.

    Args:
        cache_dir: Directory where downloaded files are cached.
        kg: KG-level file selector.
        entities: Entity-information file selector.
        relations: Relation/edge file selector.
        features: Entity-feature file selector.
        only_existing: Whether to skip selected files that are not cached.
        only_known: Whether to restrict computation to files already present in
            the hard-coded hash mapping.

    Returns:
        A dictionary mapping file names to computed SHA256 digests.
    """

    return TarKGLoader(cache_dir=cache_dir).compute_cached_sha256(
        kg=kg,
        entities=entities,
        relations=relations,
        features=features,
        only_existing=only_existing,
        only_known=only_known,
    )
