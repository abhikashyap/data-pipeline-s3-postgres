from loguru import logger
import sys
logger.add(sys.stderr,level="DEBUG")

logger.add("applog.log",level="DEBUG",rotation="1 MB",retention="10 days",compression="zip")

