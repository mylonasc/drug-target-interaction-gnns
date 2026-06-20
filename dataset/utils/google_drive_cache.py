#@title GoogleDriveCache (utility to avoid downloading and preproc in every restart)
from pathlib import Path
import subprocess
from typing import Optional, Dict


class GoogleDriveCache:
    """
    Cache local Colab files/folders to Google Drive as tar archives,
    with MD5 sidecar files for integrity checks.

    Assumes Google Drive is already mounted, e.g.:

        from google.colab import drive
        drive.mount("/content/drive")
    """

    def __init__(
        self,
        drive_cache_dir: str = "/content/drive/MyDrive/colab_cache",
        local_work_dir: str = "/content/google_drive_cache_work",
        colab_load_dir: str = "/content/restored_from_drive",
        compress: bool = True,
        verify_on_load: bool = True,
        overwrite: bool = True,
    ):
        self.drive_cache_dir = Path(drive_cache_dir)
        self.local_work_dir = Path(local_work_dir)
        self.colab_load_dir = Path(colab_load_dir)

        self.compress = compress
        self.verify_on_load = verify_on_load
        self.overwrite = overwrite

        self.drive_cache_dir.mkdir(parents=True, exist_ok=True)
        self.local_work_dir.mkdir(parents=True, exist_ok=True)
        self.colab_load_dir.mkdir(parents=True, exist_ok=True)

    def _run(self, cmd):
        """
        Run a subprocess command and capture stdout/stderr.
        Raises RuntimeError with useful error output if the command fails.
        """
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                "Command failed:\n"
                f"{' '.join(map(str, cmd))}\n\n"
                f"Return code: {result.returncode}\n\n"
                f"STDOUT:\n{result.stdout}\n\n"
                f"STDERR:\n{result.stderr}"
            )

        return result

    def _md5sum(self, path: Path) -> str:
        result = self._run(["md5sum", str(path)])
        return result.stdout.split()[0]

    def _archive_name(self, colab_path: Path) -> str:
        suffix = ".tar.gz" if self.compress else ".tar"
        return f"{colab_path.name}{suffix}"

    def _resolve_drive_archive_path(self, drive_path: str) -> Path:
        """
        Allows either:
          - absolute Drive path: /content/drive/MyDrive/colab_cache/data.tar.gz
          - relative cache name: data.tar.gz
        """
        path = Path(drive_path)

        if path.is_absolute():
            return path

        return self.drive_cache_dir / path

    def tar_and_cache_to_drive(self, colab_path: str) -> Dict[str, str]:
        """
        Tar a local Colab file/folder, compute MD5, and copy archive + .md5
        sidecar file to the configured Google Drive cache folder.
        """
        colab_path = Path(colab_path).resolve()

        if not colab_path.exists():
            raise FileNotFoundError(f"Path does not exist: {colab_path}")

        archive_name = self._archive_name(colab_path)
        local_archive = self.local_work_dir / archive_name
        drive_archive = self.drive_cache_dir / archive_name

        local_md5_file = self.local_work_dir / f"{archive_name}.md5"
        drive_md5_file = self.drive_cache_dir / f"{archive_name}.md5"

        if not self.overwrite:
            if local_archive.exists():
                raise FileExistsError(f"Local archive already exists: {local_archive}")
            if drive_archive.exists():
                raise FileExistsError(f"Drive archive already exists: {drive_archive}")

        tar_flags = "-czf" if self.compress else "-cf"

        # Archive only the basename, while running tar from the parent folder.
        # This avoids storing absolute paths like /content/data/... inside the archive.
        self._run([
            "tar",
            tar_flags,
            str(local_archive),
            "-C",
            str(colab_path.parent),
            colab_path.name,
        ])

        md5 = self._md5sum(local_archive)

        local_md5_file.write_text(f"{md5}  {archive_name}\n")

        self.drive_cache_dir.mkdir(parents=True, exist_ok=True)

        self._run(["cp", "-f", str(local_archive), str(drive_archive)])
        self._run(["cp", "-f", str(local_md5_file), str(drive_md5_file)])

        return {
            "source_colab_path": str(colab_path),
            "local_archive": str(local_archive),
            "drive_archive": str(drive_archive),
            "drive_md5_file": str(drive_md5_file),
            "md5": md5,
        }

    def untar_and_load(
        self,
        drive_path: str,
        restore_to_path: Optional[str] = None,
        check_integrity: Optional[bool] = None,
        strip_top_level_dir: bool = False,
    ) -> Dict[str, str]:
        """
        Copy a tar archive from Google Drive to local Colab storage,
        optionally verify its MD5, and extract it.

        Parameters
        ----------
        drive_path:
            Full Drive path or filename relative to self.drive_cache_dir.

        restore_to_path:
            Local Colab directory where files should be restored.
            If omitted, self.colab_load_dir is used.

        check_integrity:
            Whether to check the .md5 sidecar file before extracting.

        strip_top_level_dir:
            If False:
                archive images.tar.gz containing images/... extracts to:
                    restore_to_path/images/...

            If True:
                archive images.tar.gz containing images/... extracts contents directly to:
                    restore_to_path/...
        """
        if check_integrity is None:
            check_integrity = self.verify_on_load

        drive_archive = self._resolve_drive_archive_path(drive_path)

        if not drive_archive.exists():
            raise FileNotFoundError(f"Drive archive does not exist: {drive_archive}")

        restore_dir = Path(restore_to_path) if restore_to_path else self.colab_load_dir
        restore_dir.mkdir(parents=True, exist_ok=True)

        local_archive = self.local_work_dir / drive_archive.name
        self._run(["cp", "-f", str(drive_archive), str(local_archive)])

        md5 = self._md5sum(local_archive)

        expected_md5 = None
        if check_integrity:
            drive_md5_file = Path(f"{drive_archive}.md5")

            if not drive_md5_file.exists():
                raise FileNotFoundError(f"MD5 sidecar file not found: {drive_md5_file}")

            expected_md5 = drive_md5_file.read_text().strip().split()[0]

            if md5 != expected_md5:
                raise ValueError(
                    "MD5 integrity check failed.\n"
                    f"Archive:  {drive_archive}\n"
                    f"Expected: {expected_md5}\n"
                    f"Actual:   {md5}"
                )

        if local_archive.name.endswith(".tar.gz") or local_archive.name.endswith(".tgz"):
            tar_flags = "-xzf"
        else:
            tar_flags = "-xf"

        cmd = [
            "tar",
            tar_flags,
            str(local_archive),
            "-C",
            str(restore_dir),
        ]

        if strip_top_level_dir:
            cmd.extend(["--strip-components=1"])

        self._run(cmd)

        return {
            "drive_archive": str(drive_archive),
            "local_archive": str(local_archive),
            "restore_dir": str(restore_dir),
            "md5": md5,
            "expected_md5": expected_md5,
            "integrity_checked": str(check_integrity),
            "strip_top_level_dir": str(strip_top_level_dir),
        }
