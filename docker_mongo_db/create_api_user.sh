#!/bin/bash

# Create API user
echo "Creating API user..."
mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --eval "
    db.getSiblingDB('$API_MONGO_DB_NAME').createUser({
        user: '$API_MONGO_LOGIN',
        pwd: '$API_MONGO_PWD',
        roles: [
            { role: 'dbAdmin', db: '$API_MONGO_DB_NAME' },
            { role: 'readWrite', db: '$API_MONGO_DB_NAME' }
        ]
    });
"

# Check if created
if [ $? -ne 0 ]; then
  echo "API user creation failed."
  echo "Error occurred during MongoDB user creation."
  exit 1
fi

echo "API user $API_MONGO_LOGIN created successfully!"
