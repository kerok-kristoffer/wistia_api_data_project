import os
from dataclasses import dataclass
from typing import List


def _parse_list(val: str | None) -> List[str]:
    if not val:
        return []
    return [x.strip() for x in val.split(",") if x.strip()]


@dataclass(frozen=True)
class Settings:
    base_url: str
    api_token: str
    media_ids: List[str]
    s3_bucket_raw: str
    s3_prefix_raw: str
    ddb_table_watermark: str
    request_timeout_s: float = 15.0
    page_size: int = 100

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            base_url=os.getenv("WISTIA_BASE_URL", "https://api.wistia.com/v1"),
            api_token=os.getenv("WISTIA_API_TOKEN", ""),  # empty in unit tests
            media_ids=_parse_list(os.getenv("MEDIA_IDS")),
            s3_bucket_raw=os.getenv("S3_BUCKET_RAW", "my-bucket"),
            s3_prefix_raw=os.getenv("S3_PREFIX_RAW", "raw/wistia"),
            ddb_table_watermark=os.getenv("DDB_TABLE_WATERMARK", "wistia_watermarks"),
            request_timeout_s=float(os.getenv("REQUEST_TIMEOUT_S", "15")),
            page_size=int(os.getenv("PAGE_SIZE", "100")),
        )
