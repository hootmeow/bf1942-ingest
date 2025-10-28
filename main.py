import asyncio
import logging
from engine.scheduler import Scheduler
from engine.database import Database


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

logger = logging.getLogger(__name__)

async def main():
    db_manager = Database()
    await db_manager.connect()

    scheduler = Scheduler(db_manager)
    
    try:
        await scheduler.run()
    except asyncio.CancelledError:
        logger.info("Scheduler run cancelled.")
    finally:
        await db_manager.disconnect()
        logger.info("Application shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user.")
