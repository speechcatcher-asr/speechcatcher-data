import unicodedata

# a few problematic unicode characters that we replace with ASCII chars

CHAR_REPLACEMENTS = {
    '\u200b': ' ',   # ZERO WIDTH SPACE
    '\u200c': ' ',   # ZERO WIDTH NON-JOINER
    '\u202c': ' ',   # POP DIRECTIONAL FORMATTING
    '\u00ad': '-',   # SOFT HYPHEN
    '\u0007': ' ',   # BELL character
    '\u200e': ' ',   # LEFT-TO-RIGHT MARK
    '\ue000': ' ',   # PRIVATE USE AREA CHAR
}


def clean_line(line):
    cleaned = []
    for c in line:
        if c == '\n':
            cleaned.append(c)  # preserve newline
        elif c in CHAR_REPLACEMENTS:
            cleaned.append(CHAR_REPLACEMENTS[c])
        elif unicodedata.category(c).startswith('C'):
            cleaned.append(' ')
        else:
            cleaned.append(c)
    return ''.join(cleaned)
