class MarkerError(Exception):
    """Raised when Marker processing fails."""


class InvalidFileError(Exception):
    """Raised for invalid uploads (non-PDF, too large, corrupt)."""
