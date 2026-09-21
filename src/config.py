from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

KT1_RAW_DIR = DATA_DIR / "raw" / "kt1" / "KT1"
SAMPLE_FILE = DATA_DIR / "sample" / "kt1_sample.csv"
STREAM_INPUT_DIR = DATA_DIR / "stream_input"
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints"
OUTPUT_DIR = DATA_DIR / "output"

KT1_COLUMNS = ["timestamp", "solving_id", "question_id", "user_answer", "elapsed_time"]
KT1_STREAM_COLUMNS = KT1_COLUMNS + ["user_id"]

RANDOM_SEED = 42

DEFAULT_N_USERS = 500
DEFAULT_BATCH_ROWS = 200
DEFAULT_FEED_INTERVAL_SEC = 2.0
DEFAULT_TRIGGER_INTERVAL_SEC = 5
DEFAULT_MAX_FILES_PER_TRIGGER = 1
DEFAULT_WINDOW_DURATION = "1 hour"
DEFAULT_WATERMARK_DELAY = "1 day"
