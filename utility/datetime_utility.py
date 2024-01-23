import re


def extract_from_date_str(date_str: str):
    date_pat = r"(?P<year>\d{4})-?(?P<month>\d{2})-?(?P<day>\d{2}).*"
    datetime_pat = r"(?P<year>\d{4})-?(?P<month>\d{2})-?(?P<day>\d{2}).*(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<sec>\d{2}).*"

    if fm := re.fullmatch(datetime_pat, date_str):
        return (fm["year"], fm["month"], fm["day"], fm["hour"], fm["minute"], fm["sec"])
    elif fm := re.fullmatch(date_pat, date_str):
        return (fm["year"], fm["month"], fm["day"])
    return None


def transform_date_str_with_dash(date_str: str) -> str:  # type: ignore
    date_pat = r"\d{4}-\d{2}-\d{2}.*"
    match = re.match(date_pat, date_str)
    if match:
        return date_str
    else:
        info = extract_from_date_str(date_str)
        if info:
            return f"{info[0]}-{info[1]}-{info[2]}"
