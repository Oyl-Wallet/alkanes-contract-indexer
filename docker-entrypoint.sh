#!/bin/sh

# Exit immediately if a command exits with a non-zero status.
set -e

# Wait for the database to be ready
until pg_isready -h 127.0.0.1 -p 5555 -U $POSTGRES_USER; do
  echo "Waiting for database..."
  sleep 2
done

echo "Database is ready."

# Run migrations
# We use --source and the path to the migrations directory
# The DATABASE_URL is already in the environment
echo "Running database migrations..."
echo "Listing root directory contents:"
ls -la /
echo "Listing migrations directory contents:"
ls -la /migrations
sqlx database setup --database-url "$DATABASE_URL" --source /migrations

echo "Migrations complete."

# Execute the main command for the container
exec "$@"
