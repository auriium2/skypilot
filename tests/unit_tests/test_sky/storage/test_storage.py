"""Unit tests for storage mount commands."""

from types import SimpleNamespace

from sky.data import mounting_utils
from sky.data import storage


def test_gcs_mount_version_check_reads_stderr(monkeypatch):
    captured_version_check = None

    monkeypatch.setattr(mounting_utils, 'get_gcs_mount_install_cmd',
                        lambda: 'install-gcsfuse')
    monkeypatch.setattr(mounting_utils, 'get_gcs_mount_cmd',
                        lambda *args, **kwargs: 'mount-gcs')

    def capture_mounting_command(mount_path, install_cmd, mount_cmd,
                                 version_check_cmd):
        del mount_path, install_cmd, mount_cmd
        nonlocal captured_version_check
        captured_version_check = version_check_cmd
        return 'mount-command'

    monkeypatch.setattr(mounting_utils, 'get_mounting_command',
                        capture_mounting_command)

    store = object.__new__(storage.GcsStore)
    store.bucket = SimpleNamespace(name='test-bucket')
    store._bucket_sub_path = None
    assert store.mount_command('/mnt/test') == 'mount-command'

    assert captured_version_check == (
        f'gcsfuse --version 2>&1 | grep -q {mounting_utils.GCSFUSE_VERSION}')
