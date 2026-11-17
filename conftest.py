"""Root conftest: never collect inside data/, a WSL symlink to DATA_ROOT that Windows
cannot stat (Python raises WinError 1920 instead of returning False)."""

collect_ignore = ["data"]
