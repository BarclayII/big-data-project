
import re

def fullmatch(regex, s):
    return re.match(regex + '\\Z', s)

def isnull(x):
    return (
            x is None or                            # None
            (x == '') or                            # Empty string
            (isinstance(x, str) and x.isspace())    # Whitespace string
            )

def isdate(x):
    try:
        # The date info is pretty clean since it is always in MM/DD/YYYY form.
        # We leave the job of determining whether it is a valid date to
        # Python's datetime module.
        month, day, year = x.split('/')
        date = datetime.date(int(year), int(month), int(day))
        return True
    except:
        return False

def istime(x):
    try:
        # Same logic as isdate()
        hour, minute, second = x.split(':')
        time = datetime.time(int(hour), int(minute), int(second))
        return True
    except:
        return False
