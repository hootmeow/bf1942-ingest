import asyncio
import argparse
import asyncpg
from engine.config import settings

async def list_exclusions(pool, exclusion_type):
    """Lists all exclusions, optionally filtered by type."""
    print("--- Current Exclusions ---")
    if exclusion_type:
        rows = await pool.fetch("SELECT id, type, value, notes FROM exclusions WHERE type = $1 ORDER BY type, value;", exclusion_type)
    else:
        rows = await pool.fetch("SELECT id, type, value, notes FROM exclusions ORDER BY type, value;")

    if not rows:
        print("No exclusions found.")
        return

    for row in rows:
        print(f"ID: {row['id']:<4} Type: {row['type']:<20} Value: {row['value']:<25} Notes: {row['notes'] or ''}")

async def add_exclusion(pool, exclusion_type, value, notes):
    """Adds a new exclusion to the database."""
    try:
        await pool.execute(
            "INSERT INTO exclusions (type, value, notes) VALUES ($1, $2, $3);",
            exclusion_type, value, notes
        )
        print(f"✅ Successfully added exclusion: [Type: {exclusion_type}, Value: {value}]")
    except asyncpg.exceptions.UniqueViolationError:
        print(f"⚠️ Error: An exclusion for [Type: {exclusion_type}, Value: {value}] already exists.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

async def remove_exclusion(pool, exclusion_id):
    """Removes an exclusion by its ID."""
    result = await pool.execute("DELETE FROM exclusions WHERE id = $1;", exclusion_id)
    if result == 'DELETE 1':
        print(f"✅ Successfully removed exclusion with ID: {exclusion_id}")
    else:
        print(f"⚠️ Error: No exclusion found with ID: {exclusion_id}")

async def main():
    """Main function to parse arguments and run commands."""
    parser = argparse.ArgumentParser(description="Manage the BF1942 stats exclusion list.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List all current exclusions.")
    list_parser.add_argument("--type", help="Filter list by type (e.g., 'gametype', 'player_name', 'server_id').")

    add_parser = subparsers.add_parser("add", help="Add a new exclusion.")
    # Add 'server_id' to the list of choices
    add_parser.add_argument("type", choices=['gametype', 'player_name', 'server_id'], help="The type of exclusion.")
    add_parser.add_argument("value", help="The value to exclude (e.g., 'coop', 'AFK_Bot', or '1.2.3.4:23000').")
    add_parser.add_argument("--notes", help="Optional notes for the exclusion.")

    remove_parser = subparsers.add_parser("remove", help="Remove an exclusion by its ID.")
    remove_parser.add_argument("id", type=int, help="The numeric ID of the exclusion to remove (use 'list' to find it).")

    args = parser.parse_args()
    
    pool = await asyncpg.create_pool(dsn=settings.POSTGRES_DSN)

    if args.command == "list":
        await list_exclusions(pool, args.type)
    elif args.command == "add":
        await add_exclusion(pool, args.type, args.value, args.notes)
    elif args.command == "remove":
        await remove_exclusion(pool, args.id)

    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
