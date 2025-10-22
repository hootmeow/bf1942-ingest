import asyncio
from engine.scheduler import Scheduler
from engine.database import Database

async def main():
    db_manager = Database()
    await db_manager.connect()

    scheduler = Scheduler(db_manager)
    
    try:
        await scheduler.run()
    except asyncio.CancelledError:
        print("Scheduler run cancelled.")
    finally:
        await db_manager.disconnect()
        print("Application shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested by user.")
