"""
Centralized cross-platform filename handling utilities.
"""
import os
import re
from typing import Optional


# Windows reserved device names (case-insensitive)
WINDOWS_RESERVED_NAMES = re.compile(
    r'^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\.|$)',
    re.IGNORECASE
)

# Characters not allowed in Windows filenames
WINDOWS_BAD_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1F]')

# Characters to avoid on Unix systems
UNIX_BAD_CHARS = [';', ':', '!', '*', '/', '\\']


def sanitize_filename(title: str, max_length: int = 200) -> str:
    """
    Sanitize a filename to be safe for both Windows and Unix systems.

    Args:
        title: The original filename/title to sanitize
        max_length: Maximum length of the sanitized filename (default 200)

    Returns:
        A sanitized filename safe for use on any platform
    """
    if not title:
        return "untitled"

    clean_title = title

    if os.name == 'nt':
        # Windows: replace bad characters with underscore
        clean_title = WINDOWS_BAD_CHARS.sub('_', clean_title)
        # Handle reserved names by prefixing with underscore
        if WINDOWS_RESERVED_NAMES.match(clean_title):
            clean_title = '_' + clean_title
        # Remove leading/trailing spaces and dots (Windows doesn't allow trailing dots)
        clean_title = clean_title.strip(' .')
    else:
        # Unix: remove problematic characters
        clean_title = ''.join(ch if ch not in UNIX_BAD_CHARS else '_' for ch in clean_title)
        clean_title = clean_title.strip()

    # Truncate if too long (preserve some room for extension)
    if len(clean_title) > max_length:
        clean_title = clean_title[:max_length]

    # Fallback if empty after sanitization
    if not clean_title:
        clean_title = "untitled"

    return clean_title


def create_filename(
    output_directory: str,
    content_document_id: str,
    title: str,
    file_extension: str,
    linked_entity_name: Optional[str] = None,
    version_number: Optional[str] = None,
    filename_pattern: str = '{0}{1}-{2}.{3}'
) -> str:
    """
    Create a filename based on a pattern with placeholders.

    Placeholders:
        {0} = output_directory (with trailing separator)
        {1} = content_document_id
        {2} = title (sanitized)
        {3} = file_extension
        {4} = linked_entity_name (sanitized)
        {5} = version_number

    Args:
        output_directory: Directory where the file will be saved
        content_document_id: Salesforce ContentDocument ID
        title: File title (will be sanitized)
        file_extension: File extension (without dot)
        linked_entity_name: Name of the linked entity (optional)
        version_number: ContentVersion version number (optional)
        filename_pattern: Pattern string with placeholders

    Returns:
        Complete file path with sanitized filename
    """
    # Ensure output directory ends with separator
    if output_directory and not output_directory.endswith(os.sep):
        output_directory = output_directory + os.sep

    # Sanitize title and linked entity name
    clean_title = sanitize_filename(title)
    clean_linked_entity_name = sanitize_filename(linked_entity_name) if linked_entity_name else ''

    # Handle None values
    version_str = str(version_number) if version_number else ''

    # Build the filename using the pattern
    filename = filename_pattern.format(
        output_directory,           # {0}
        content_document_id,        # {1}
        clean_title,                # {2}
        file_extension,             # {3}
        clean_linked_entity_name,   # {4}
        version_str                 # {5}
    )

    return filename
